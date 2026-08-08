"""Message Bus: filesystem-based event tracking with time-bucketed rollups."""
import calendar
import datetime
import json
import threading
from omigo_core import utils, timefuncs, dataframe
from omigo_hydra.cluster_common_v2 import ClusterPaths
from omigo_hydra import cluster_arjun, etl

# Default namespace for system-level events
MESSAGE_BUS_DEFAULT_NAMESPACE = "platform"

# Cleanup threshold in seconds before deleting processed messages (S3 eventual consistency)
MESSAGE_BUS_CLEANUP_THRESHOLD_SECONDS = 300

# Late arrival directory name (messages arriving after their 01min window was rolled up)
MESSAGE_BUS_INCOMING_LATE = "incoming-late"

# Grace period in seconds before writing empty sentinel files for windows with no data
MESSAGE_BUS_EMPTY_WINDOW_GRACE_SECONDS = 60

# Grace period in seconds before sealing a 01min window into a bucket file.
# Events arriving within this period after the window ends are still included
# in the bucket rather than being routed to incoming-late/.
MESSAGE_BUS_ROLLUP_GRACE_SECONDS = 60

# Bucket level constants
BUCKET_01MIN = "01min"
BUCKET_05MIN = "05min"
BUCKET_01HRS = "01hrs"
BUCKET_04HRS = "04hrs"
BUCKET_08HRS = "08hrs"
BUCKET_12HRS = "12hrs"
BUCKET_01DAY = "01day"

# Bucket levels in ascending order
MESSAGE_BUS_BUCKET_LEVELS = [BUCKET_01MIN, BUCKET_05MIN, BUCKET_01HRS, BUCKET_04HRS, BUCKET_08HRS, BUCKET_12HRS, BUCKET_01DAY]

# Interval in seconds for each bucket level
MESSAGE_BUS_BUCKET_INTERVALS_SECONDS = {
    BUCKET_01MIN: 60,
    BUCKET_05MIN: 300,
    BUCKET_01HRS: 3600,
    BUCKET_04HRS: 14400,
    BUCKET_08HRS: 28800,
    BUCKET_12HRS: 43200,
    BUCKET_01DAY: 86400,
}

# Cascade configuration: level -> (source_level, multiplier)
# Each level reads from the largest lower level that divides evenly.
# "incoming-current" means raw tsv.gz dataframe messages from the incoming-current folder.
MESSAGE_BUS_BUCKET_CASCADE = {
    BUCKET_01MIN: ("incoming-current", 1),
    BUCKET_05MIN: (BUCKET_01MIN, 5),
    BUCKET_01HRS: (BUCKET_05MIN, 12),
    BUCKET_04HRS: (BUCKET_01HRS, 4),
    BUCKET_08HRS: (BUCKET_04HRS, 2),
    BUCKET_12HRS: (BUCKET_04HRS, 3),
    BUCKET_01DAY: (BUCKET_12HRS, 2),
}

# Levels in descending order for read API (greedy top-down)
MESSAGE_BUS_BUCKET_LEVELS_DESC = list(reversed(MESSAGE_BUS_BUCKET_LEVELS))

# Column schema for message bus dataframes (incoming-current + bucket files share same schema)
MESSAGE_BUS_COLUMNS = [
    "message_id", "namespace", "entity_type", "entity_id", "message_type",
    "output_id", cluster_arjun.OMIGO_ARJUN_EVENT_TS, "counter", "json_payload"
]


class MessageFileName:
    """Structured representation of a message bus filename.

    Format: {namespace}.{entity_type}.{entity_id}.{message_type}.{YYYYmmDDHHMMSS}.{millis}.{counter}
    File:   {message_filename}.tsv.gz
    """

    def __init__(self, namespace, entity_type, entity_id, message_type, ts_date, ts_millis, counter):
        self.namespace = namespace
        self.entity_type = entity_type
        self.entity_id = entity_id
        self.message_type = message_type
        self.ts_date = ts_date          # YYYYmmDDHHMMSS string
        self.ts_millis = ts_millis      # int, 0-999
        self.counter = counter          # int

    def to_string(self):
        """Serialize to dot-separated message filename string."""
        return "{}.{}.{}.{}.{}.{:03d}.{:04d}".format(
            self.namespace, self.entity_type, self.entity_id, self.message_type,
            self.ts_date, self.ts_millis, self.counter)

    def to_filename(self):
        """Serialize to full filename with .tsv.gz extension."""
        return "{}.tsv.gz".format(self.to_string())

    def get_epoch_seconds(self):
        """Convert ts_date + ts_millis to epoch seconds (float)."""
        dt = datetime.datetime.strptime(self.ts_date, "%Y%m%d%H%M%S")
        return calendar.timegm(dt.timetuple()) + self.ts_millis / 1000.0

    def parse(s):
        """Parse a dot-separated message filename string (without .tsv.gz extension).

        Format: {namespace}.{entity_type}.{entity_id}.{message_type}.{YYYYmmDDHHMMSS}.{millis}.{counter}
        Note: entity_id may contain dots, so we parse from both ends.
        """
        parts = s.split(".")
        # fixed positions from the end: counter[-1], millis[-2], ts_date[-3], message_type[-4]
        # fixed positions from the start: namespace[0], entity_type[1]
        # everything in between is entity_id (may contain dots)
        counter = int(parts[-1])
        ts_millis = int(parts[-2])
        ts_date = parts[-3]
        message_type = parts[-4]
        namespace = parts[0]
        entity_type = parts[1]
        entity_id = ".".join(parts[2:-4])
        return MessageFileName(namespace, entity_type, entity_id, message_type, ts_date, ts_millis, counter)

    def parse_filename(filename):
        """Parse from a full filename (strip .tsv.gz, then parse)."""
        s = filename.rsplit(".tsv.gz", 1)[0]
        return MessageFileName.parse(s)

    def generate(namespace, entity_type, entity_id, message_type, counter):
        """Create a new MessageFileName with the current UTC timestamp."""
        from omigo_hydra.cluster_common_v2 import __millis_to_ts_str__
        ts_millis_total = timefuncs.get_utctimestamp_millis()
        ts_str = __millis_to_ts_str__(ts_millis_total)
        # ts_str is "YYYYmmDDHHMMSS.mmm"
        ts_parts = ts_str.split(".")
        ts_date = ts_parts[0]
        ts_millis = int(ts_parts[1])
        return MessageFileName(namespace, entity_type, entity_id, message_type, ts_date, ts_millis, counter)


class MessageBus:
    def __init__(self, cluster_handler):
        self.cluster_handler = cluster_handler
        self._counter = 0
        self._counter_lock = threading.Lock()

    def __next_counter__(self):
        with self._counter_lock:
            self._counter += 1
            return self._counter

    def publish(self, namespace, entity_type, entity_id, message_type, payload, output_id = "", dmsg = ""):
        dmsg = utils.extend_inherit_message(dmsg, "MessageBus.publish")

        # default namespace
        if (namespace is None or namespace == ""):
            namespace = MESSAGE_BUS_DEFAULT_NAMESPACE

        # generate unique message filename
        counter = self.__next_counter__()
        msg_file_name = MessageFileName.generate(namespace, entity_type, entity_id, message_type, counter)
        message_id = msg_file_name.to_string()
        event_ts = msg_file_name.get_epoch_seconds()

        # build single-row dataframe with columnar schema
        row = [message_id, namespace, entity_type, entity_id, message_type, str(output_id), str(event_ts), str(counter), json.dumps(payload)]
        xdf = dataframe.new_with_cols(MESSAGE_BUS_COLUMNS, [row])

        # write to incoming as tsv.gz
        file_path = ClusterPaths.get_message_bus_message_file(message_id)
        self.cluster_handler.write_df(file_path, xdf)

        utils.trace("{}: published message_id: {}".format(dmsg, message_id))
        return message_id

    def read(self, namespace, start_ts, end_ts, message_type = None, dmsg = ""):
        """Greedy top-down read: largest buckets first, fill gaps with smaller, dedup on message_id."""
        dmsg = utils.extend_inherit_message(dmsg, "MessageBus.read")

        if (namespace is None or namespace == ""):
            namespace = MESSAGE_BUS_DEFAULT_NAMESPACE

        all_dfs = []

        # iterate from largest bucket to smallest
        for level in MESSAGE_BUS_BUCKET_LEVELS_DESC:
            interval = MESSAGE_BUS_BUCKET_INTERVALS_SECONDS[level]
            bucket_start = etl.floor_to_bucket(start_ts, interval)

            while (bucket_start < end_ts):
                bucket_end = bucket_start + interval
                bucket_file = ClusterPaths.get_message_bus_bucket_etl_file(level, bucket_start, bucket_end)

                if (self.cluster_handler.file_exists(bucket_file)):
                    xdf = self.cluster_handler.read_df(bucket_file)
                    if (xdf.num_rows() > 0):
                        all_dfs.append(xdf)

                bucket_start = bucket_end

        # read incoming tsv.gz files for very recent data
        incoming_path = ClusterPaths.get_message_bus_incoming_current()
        if (self.cluster_handler.dir_exists(incoming_path)):
            files = self.cluster_handler.list_leaf_dir(incoming_path)
            for f in files:
                file_path = "{}/{}".format(incoming_path, f)
                xdf = self.cluster_handler.read_df(file_path)
                if (xdf.num_rows() > 0):
                    all_dfs.append(xdf)

        # merge all
        if (len(all_dfs) == 0):
            return dataframe.new_with_cols(MESSAGE_BUS_COLUMNS)

        xdf_merged = dataframe.merge_union(all_dfs, dmsg = dmsg)

        # dedup on message_id using native distinct
        if (xdf_merged.num_rows() > 0):
            xdf_merged = xdf_merged.distinct("message_id")

        # filter by message_type if specified
        if (message_type is not None and xdf_merged.num_rows() > 0):
            cols = xdf_merged.get_columns()
            col_arrays = [xdf_merged.col_as_array(c) for c in cols]
            msg_type_values = xdf_merged.col_as_array("message_type")
            filtered_rows = []
            for i in range(len(msg_type_values)):
                if (msg_type_values[i] == message_type):
                    filtered_rows.append([col_arrays[j][i] for j in range(len(cols))])
            xdf_merged = dataframe.new_with_cols(cols, filtered_rows)

        utils.trace("{}: read {} rows for [{}, {})".format(dmsg, xdf_merged.num_rows(), start_ts, end_ts))
        return xdf_merged


class BucketRollup:
    """Standalone rollup logic for message bus buckets. Designed for future extraction into etl.py."""

    def get_source_level(self, level):
        if (level not in MESSAGE_BUS_BUCKET_CASCADE):
            raise Exception("BucketRollup: invalid bucket level: {}".format(level))
        return MESSAGE_BUS_BUCKET_CASCADE[level][0]

    def get_multiplier(self, level):
        if (level not in MESSAGE_BUS_BUCKET_CASCADE):
            raise Exception("BucketRollup: invalid bucket level: {}".format(level))
        return MESSAGE_BUS_BUCKET_CASCADE[level][1]

    def get_interval_seconds(self, level):
        if (level not in MESSAGE_BUS_BUCKET_INTERVALS_SECONDS):
            raise Exception("BucketRollup: invalid bucket level: {}".format(level))
        return MESSAGE_BUS_BUCKET_INTERVALS_SECONDS[level]

    def should_prune_source(self, level):
        """Only 05min prunes its source (01min files). All other levels keep sources."""
        return level == BUCKET_05MIN

    def get_expected_source_paths(self, namespace, level, start_ts, end_ts):
        """Compute expected source ETL file paths for a rollup step.

        For level 05min with start_ts at 00:00:00 and end_ts at 00:05:00,
        returns five 01min ETL file paths: [00:00-00:01, 00:01-00:02, ..., 00:04-00:05].
        """
        source_level = self.get_source_level(level)

        # 01min reads from incoming (tsv.gz), not from ETL files
        if (source_level == "incoming-current"):
            raise Exception("BucketRollup: 01min reads from incoming-current, not ETL files. Use rollup_01min() instead.")

        source_interval = self.get_interval_seconds(source_level)
        source_base = ClusterPaths.get_message_bus_bucket(source_level)

        # compute paths using the same logic as etl.get_expected_upstream_etl_file_paths
        paths = []
        bucket_start = etl.floor_to_bucket(start_ts, source_interval)
        while (bucket_start < end_ts):
            bucket_end = bucket_start + source_interval
            dt_str = etl.get_etl_file_date_str_from_ts(bucket_start)
            base_name = etl.get_etl_file_base_name_by_ts(ClusterPaths.DEFAULT_OUTPUT_PREFIX, bucket_start, bucket_end)
            paths.append("{}/dt={}/{}.tsv.gz".format(source_base, dt_str, base_name))
            bucket_start = bucket_end

        return paths

    def rollup_01min(self, cluster_handler, namespace, start_ts, end_ts, dmsg = ""):
        """Read incoming-current tsv.gz dataframes, detect late arrivals, merge rest into 01min bucket file."""
        dmsg = utils.extend_inherit_message(dmsg, "BucketRollup.rollup_01min")

        incoming_path = ClusterPaths.get_message_bus_incoming_current()

        # list all tsv.gz files in incoming-current
        if (cluster_handler.dir_exists(incoming_path) == False):
            return None

        files = cluster_handler.list_leaf_dir(incoming_path)

        if (len(files) == 0):
            utils.trace("{}: no messages in incoming-current, skipping".format(dmsg))
            return None

        # build set of completed message IDs for dedup
        completed_path = ClusterPaths.get_message_bus_completed()
        completed_files = set()
        if (cluster_handler.dir_exists(completed_path)):
            for cf in cluster_handler.list_leaf_dir(completed_path):
                completed_files.add(cf)

        # read each incoming-current dataframe and separate late arrivals
        xdf_list = []
        on_time_files = []
        late_count = 0
        skipped_count = 0
        interval_01min = MESSAGE_BUS_BUCKET_INTERVALS_SECONDS[BUCKET_01MIN]

        for f in files:
            # dedup: skip files that already have a marker in completed
            if (f in completed_files):
                skipped_count += 1
                continue

            # parse filename to extract timestamp and determine window
            msg_file_name = MessageFileName.parse_filename(f)
            msg_ts = msg_file_name.get_epoch_seconds()
            msg_window_start = etl.floor_to_bucket(msg_ts, interval_01min)
            msg_window_end = msg_window_start + interval_01min

            # check if a 01min bucket file already exists for this message's window
            existing_bucket = ClusterPaths.get_message_bus_bucket_etl_file(BUCKET_01MIN, msg_window_start, msg_window_end)
            if (cluster_handler.file_exists(existing_bucket)):
                # late arrival: move to incoming-late
                file_path = "{}/{}".format(incoming_path, f)
                xdf = cluster_handler.read_df(file_path)
                msg_id = msg_file_name.to_string()
                late_file = ClusterPaths.get_message_bus_incoming_late_file(msg_id)
                cluster_handler.write_df(late_file, xdf)
                cluster_handler.remove_file(file_path)
                late_count += 1
                utils.trace("{}: late arrival detected, moved to incoming-late: {}".format(dmsg, msg_id))
            else:
                file_path = "{}/{}".format(incoming_path, f)
                xdf = cluster_handler.read_df(file_path)
                if (xdf.num_rows() == 0):
                    continue
                xdf_list.append(xdf)
                on_time_files.append(file_path)

        if (skipped_count > 0):
            utils.trace("{}: skipped {} files already in completed".format(dmsg, skipped_count))

        if (late_count > 0):
            utils.trace("{}: moved {} late arrivals to incoming-late".format(dmsg, late_count))

        if (len(xdf_list) == 0):
            utils.trace("{}: no on-time messages after filtering, skipping".format(dmsg))
            return None

        # merge all on-time incoming-current dataframes
        xdf_merged = dataframe.merge_union(xdf_list, dmsg = dmsg)

        if (xdf_merged.num_rows() == 0):
            utils.trace("{}: no rows after merge, skipping".format(dmsg))
            return None

        # write bucket file
        bucket_dt_path = ClusterPaths.get_message_bus_bucket_etl_dt(BUCKET_01MIN, start_ts)
        cluster_handler.create(bucket_dt_path)
        bucket_file = ClusterPaths.get_message_bus_bucket_etl_file(BUCKET_01MIN, start_ts, end_ts)
        cluster_handler.write_df(bucket_file, xdf_merged)

        # verify write
        if (cluster_handler.file_exists(bucket_file) == False):
            utils.warn("{}: failed to verify bucket file: {}".format(dmsg, bucket_file))
            return None

        # write header-only marker files to completed/ (after bucket write to prevent data loss)
        cluster_handler.create(completed_path)
        for i, f_path in enumerate(on_time_files):
            f_name = f_path.rsplit("/", 1)[-1]
            msg_file_name = MessageFileName.parse_filename(f_name)
            completed_file = ClusterPaths.get_message_bus_completed_file(msg_file_name.to_string())
            xdf_marker = xdf_list[i].take(0)
            cluster_handler.write_df(completed_file, xdf_marker)

        utils.trace("{}: rolled up {} messages into {}".format(dmsg, len(on_time_files), bucket_file))
        return bucket_file

    def rollup_level(self, cluster_handler, namespace, level, start_ts, end_ts, dmsg = ""):
        """Generic rollup: read source tsv.gz files, merge, write target bucket file."""
        dmsg = utils.extend_inherit_message(dmsg, "BucketRollup.rollup_level:{}".format(level))

        # compute expected source paths
        source_paths = self.get_expected_source_paths(namespace, level, start_ts, end_ts)

        # check all source files exist
        for path in source_paths:
            if (cluster_handler.file_exists(path) == False):
                utils.trace("{}: source file missing: {}, skipping rollup".format(dmsg, path))
                return None

        # read all source files
        xdf_list = []
        for path in source_paths:
            xdf = cluster_handler.read_df(path)
            xdf_list.append(xdf)

        # merge
        xdf_merged = dataframe.merge_union(xdf_list, dmsg = dmsg)

        if (xdf_merged.num_rows() == 0):
            utils.trace("{}: merged result has zero rows, skipping".format(dmsg))
            return None

        # write target bucket file
        target_dt_path = ClusterPaths.get_message_bus_bucket_etl_dt(level, start_ts)
        cluster_handler.create(target_dt_path)
        target_file = ClusterPaths.get_message_bus_bucket_etl_file(level, start_ts, end_ts)
        cluster_handler.write_df(target_file, xdf_merged)

        # verify write
        if (cluster_handler.file_exists(target_file) == False):
            utils.warn("{}: failed to verify target file: {}".format(dmsg, target_file))
            return None

        utils.trace("{}: rolled up {} source files ({} rows) into {}".format(dmsg, len(source_paths), xdf_merged.num_rows(), target_file))
        return target_file

    def cleanup_incoming(self, cluster_handler, namespace, threshold_seconds = None, dmsg = ""):
        """Scan completed/, delete corresponding incoming-current file (ignore if missing), then delete completed marker."""
        dmsg = utils.extend_inherit_message(dmsg, "BucketRollup.cleanup_incoming")

        if (threshold_seconds is None):
            threshold_seconds = MESSAGE_BUS_CLEANUP_THRESHOLD_SECONDS

        completed_path = ClusterPaths.get_message_bus_completed()
        if (cluster_handler.dir_exists(completed_path) == False):
            return

        files = cluster_handler.list_leaf_dir(completed_path)
        now = timefuncs.get_utctimestamp_sec()
        deleted_count = 0

        for f in files:
            # parse filename to extract timestamp
            msg_file_name = MessageFileName.parse_filename(f)
            msg_ts = msg_file_name.get_epoch_seconds()

            # check if enough time has passed since the message was created
            if (now - msg_ts < threshold_seconds):
                continue

            # delete from incoming-current first (ignore if already missing — file may be concurrently removed by rollup)
            msg_id = msg_file_name.to_string()
            incoming_file = ClusterPaths.get_message_bus_message_file(msg_id)
            cluster_handler.remove_file(incoming_file, ignore_if_missing = True)

            # then delete the completed marker (ignore_if_missing for concurrent cleanup)
            completed_file = "{}/{}".format(completed_path, f)
            cluster_handler.remove_file(completed_file, ignore_if_missing = True)
            deleted_count += 1

        utils.trace("{}: cleaned up {} entries from incoming-current and completed".format(dmsg, deleted_count))

    def prune_01min(self, cluster_handler, namespace, start_ts, end_ts, threshold_seconds = None, dmsg = ""):
        """Delete 01min source files after verified 05min rollup."""
        dmsg = utils.extend_inherit_message(dmsg, "BucketRollup.prune_01min")

        if (threshold_seconds is None):
            threshold_seconds = MESSAGE_BUS_CLEANUP_THRESHOLD_SECONDS

        # verify 05min file exists
        target_file = ClusterPaths.get_message_bus_bucket_etl_file(BUCKET_05MIN, start_ts, end_ts)
        if (cluster_handler.file_exists(target_file) == False):
            utils.trace("{}: 05min file does not exist, skipping prune".format(dmsg))
            return

        # get source 01min paths
        source_paths = self.get_expected_source_paths(namespace, BUCKET_05MIN, start_ts, end_ts)
        for path in source_paths:
            if (cluster_handler.file_exists(path)):
                cluster_handler.remove_file(path)

        utils.trace("{}: pruned {} 01min files".format(dmsg, len(source_paths)))

    def rollup_all_levels(self, cluster_handler, namespace, dmsg = ""):
        """Run the full rollup cascade: incoming-current -> 01min -> ... -> 01day. Idempotent."""
        dmsg = utils.extend_inherit_message(dmsg, "BucketRollup.rollup_all_levels")
        now = timefuncs.get_utctimestamp_sec()

        # 01min: rollup the most recently completed minute from incoming-current.
        # Apply grace period so the window is not sealed until ROLLUP_GRACE seconds
        # after it ends, giving late-arriving events time to land in incoming-current/.
        interval_01min = MESSAGE_BUS_BUCKET_INTERVALS_SECONDS[BUCKET_01MIN]
        effective_now = now - MESSAGE_BUS_ROLLUP_GRACE_SECONDS
        bucket_end_01min = etl.floor_to_bucket(effective_now, interval_01min)
        bucket_start_01min = bucket_end_01min - interval_01min
        target_01min = ClusterPaths.get_message_bus_bucket_etl_file(BUCKET_01MIN, bucket_start_01min, bucket_end_01min)
        if (cluster_handler.file_exists(target_01min) == False):
            self.rollup_01min(cluster_handler, namespace, bucket_start_01min, bucket_end_01min, dmsg = dmsg)

        # 05min through 01day: rollup from source level
        for level in MESSAGE_BUS_BUCKET_LEVELS[1:]:
            interval = MESSAGE_BUS_BUCKET_INTERVALS_SECONDS[level]
            bucket_end = etl.floor_to_bucket(now, interval)
            bucket_start = bucket_end - interval
            target_file = ClusterPaths.get_message_bus_bucket_etl_file(level, bucket_start, bucket_end)
            if (cluster_handler.file_exists(target_file) == False):
                result = self.rollup_level(cluster_handler, namespace, level, bucket_start, bucket_end, dmsg = dmsg)
                # after 05min rollup succeeds, prune 01min sources
                if (result is not None and self.should_prune_source(level)):
                    self.prune_01min(cluster_handler, namespace, bucket_start, bucket_end, dmsg = dmsg)

        # cleanup incoming
        self.cleanup_incoming(cluster_handler, namespace, dmsg = dmsg)

        utils.trace("{}: rollup cascade complete".format(dmsg))

    def __get_pending_incoming_windows__(self, cluster_handler, dmsg = ""):
        """Return set of 01min window-start timestamps that have pending events in incoming-current/."""
        incoming_path = ClusterPaths.get_message_bus_incoming_current()
        if (cluster_handler.dir_exists(incoming_path) == False):
            return set()

        files = cluster_handler.list_leaf_dir(incoming_path)
        if (len(files) == 0):
            return set()

        interval_01min = MESSAGE_BUS_BUCKET_INTERVALS_SECONDS[BUCKET_01MIN]
        windows = set()
        for f in files:
            msg_file_name = MessageFileName.parse_filename(f)
            msg_ts = msg_file_name.get_epoch_seconds()
            window_start = etl.floor_to_bucket(msg_ts, interval_01min)
            windows.add(window_start)

        if (len(windows) > 0):
            utils.trace("{}: found pending incoming-current events for {} windows".format(dmsg, len(windows)))

        return windows

    def fill_empty_windows(self, cluster_handler, namespace, now = None, dmsg = ""):
        """Write header-only sentinel files for 01min windows past the grace period with no data."""
        dmsg = utils.extend_inherit_message(dmsg, "BucketRollup.fill_empty_windows")

        if (now is None):
            now = timefuncs.get_utctimestamp_sec()

        interval_01min = MESSAGE_BUS_BUCKET_INTERVALS_SECONDS[BUCKET_01MIN]
        grace = MESSAGE_BUS_EMPTY_WINDOW_GRACE_SECONDS

        # collect windows that have pending events in incoming-current/ — do not write sentinels for these
        pending_windows = self.__get_pending_incoming_windows__(cluster_handler, dmsg = dmsg)

        # start from the most recent completed window and scan backward
        latest_window_end = etl.floor_to_bucket(now, interval_01min)
        max_windows = 10  # safety limit to avoid infinite scan on first startup
        filled_count = 0

        for i in range(max_windows):
            window_end = latest_window_end - (i * interval_01min)
            window_start = window_end - interval_01min

            # skip if still within grace period
            if (now - window_end < grace):
                continue

            bucket_file = ClusterPaths.get_message_bus_bucket_etl_file(BUCKET_01MIN, window_start, window_end)
            if (cluster_handler.file_exists(bucket_file)):
                # found an existing file — all earlier windows are handled, stop
                break

            # skip if incoming-current/ has pending events for this window — let rollup_01min handle it
            if (window_start in pending_windows):
                utils.trace("{}: skipping sentinel for [{}, {}), pending events in incoming-current".format(dmsg, window_start, window_end))
                continue

            # write empty sentinel
            bucket_dt_path = ClusterPaths.get_message_bus_bucket_etl_dt(BUCKET_01MIN, window_start)
            cluster_handler.create(bucket_dt_path)
            empty_df = dataframe.new_with_cols(MESSAGE_BUS_COLUMNS)
            cluster_handler.write_df(bucket_file, empty_df)
            filled_count += 1
            utils.trace("{}: wrote empty sentinel for [{}, {})".format(dmsg, window_start, window_end))

        if (filled_count > 0):
            utils.trace("{}: filled {} empty windows".format(dmsg, filled_count))


def publish_event(cluster_handler, entity_type, entity_id, message_type, payload, output_id = "", dmsg = ""):
    """Convenience wrapper: create a throwaway MessageBus and publish one event."""
    mbus = MessageBus(cluster_handler)
    mbus.publish(MESSAGE_BUS_DEFAULT_NAMESPACE, entity_type, entity_id, message_type, payload, output_id = output_id, dmsg = dmsg)


class MessageBusRollupDF(dataframe.DataFrame):
    """Extension class for running message bus rollup cascade within a WF."""

    def rollup_cascade(self):
        cluster_handler = ClusterPaths.get_cluster_handler()
        namespace = MESSAGE_BUS_DEFAULT_NAMESPACE
        rollup = BucketRollup()
        rollup.rollup_all_levels(cluster_handler, namespace, dmsg = "MessageBusRollupDF.rollup_cascade")
        return self
