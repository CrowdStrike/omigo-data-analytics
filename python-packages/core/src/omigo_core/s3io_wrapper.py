import omigo_hydra

class S3FSWrapper:
    def __init__(self):
        pass

    def file_exists(*args, **kwargs):
        utils.rate_limit_after_n_warnings("Deprecated. Use omigo_hydra")
        return omigo_hydra.s3io_wrapper.file_exists(*args, **kwargs)

    def file_exists_with_wait(*args, **kwargs):
        utils.rate_limit_after_n_warnings("Deprecated. Use omigo_hydra")
        return omigo_hydra.s3io_wrapper.file_exists_with_wait(*args, **kwargs)

    def dir_exists(*args, **kwargs):
        utils.rate_limit_after_n_warnings("Deprecated. Use omigo_hydra")
        return omigo_hydra.s3io_wrapper.dir_exists(*args, **kwargs)

    def dir_exists_with_wait(*args, **kwargs):
        utils.rate_limit_after_n_warnings("Deprecated. Use omigo_hydra")
        return omigo_hydra.s3io_wrapper.dir_exists_with_wait(*args, **kwargs)

    def file_not_exists(*args, **kwargs):
        utils.rate_limit_after_n_warnings("Deprecated. Use omigo_hydra")
        return omigo_hydra.s3io_wrapper.file_not_exists(*args, **kwargs)

    def file_not_exists_with_wait(*args, **kwargs):
        utils.rate_limit_after_n_warnings("Deprecated. Use omigo_hydra")
        return omigo_hydra.s3io_wrapper.file_not_exists_with_wait(*args, **kwargs)

    def dir_not_exists_with_wait(*args, **kwargs):
        utils.rate_limit_after_n_warnings("Deprecated. Use omigo_hydra")
        return omigo_hydra.s3io_wrapper.dir_not_exists_with_wait(*args, **kwargs)

    def read_file_contents_as_text_with_wait(*args, **kwargs):
        utils.rate_limit_after_n_warnings("Deprecated. Use omigo_hydra")
        return omigo_hydra.s3io_wrapper.read_file_contents_as_text_with_wait(*args, **kwargs)

    def read_file_contents_as_text(*args, **kwargs):
        utils.rate_limit_after_n_warnings("Deprecated. Use omigo_hydra")
        return omigo_hydra.s3io_wrapper.read_file_contents_as_text(*args, **kwargs)

    def ls(*args, **kwargs):
        utils.rate_limit_after_n_warnings("Deprecated. Use omigo_hydra")
        return omigo_hydra.s3io_wrapper.ls(*args, **kwargs)

    def get_directory_listing(*args, **kwargs):
        utils.rate_limit_after_n_warnings("Deprecated. Use omigo_hydra")
        return omigo_hydra.s3io_wrapper.get_directory_listing(*args, **kwargs)

    def list_dirs(*args, **kwargs):
        utils.rate_limit_after_n_warnings("Deprecated. Use omigo_hydra")
        return omigo_hydra.s3io_wrapper.list_dirs(*args, **kwargs)

    def list_files(*args, **kwargs):
        utils.rate_limit_after_n_warnings("Deprecated. Use omigo_hydra")
        return omigo_hydra.s3io_wrapper.list_files(*args, **kwargs)

    def is_file(*args, **kwargs):
        utils.rate_limit_after_n_warnings("Deprecated. Use omigo_hydra")
        return omigo_hydra.s3io_wrapper.is_file(*args, **kwargs)

    def is_directory(*args, **kwargs):
        utils.rate_limit_after_n_warnings("Deprecated. Use omigo_hydra")
        return omigo_hydra.s3io_wrapper.is_directory(*args, **kwargs)

    def delete_file_with_wait(*args, **kwargs):
        utils.rate_limit_after_n_warnings("Deprecated. Use omigo_hydra")
        return omigo_hydra.s3io_wrapper.delete_file_with_wait(*args, **kwargs)

    def delete_file(*args, **kwargs):
        utils.rate_limit_after_n_warnings("Deprecated. Use omigo_hydra")
        return omigo_hydra.s3io_wrapper.delete_file(*args, **kwargs)

    def delete_dir_with_wait(*args, **kwargs):
        utils.rate_limit_after_n_warnings("Deprecated. Use omigo_hydra")
        return omigo_hydra.s3io_wrapper.delete_dir_with_wait(*args, **kwargs)

    def get_parent_directory(*args, **kwargs):
        utils.rate_limit_after_n_warnings("Deprecated. Use omigo_hydra")
        return omigo_hydra.s3io_wrapper.get_parent_directory(*args, **kwargs)

    def write_text_file(*args, **kwargs):
        utils.rate_limit_after_n_warnings("Deprecated. Use omigo_hydra")
        return omigo_hydra.s3io_wrapper.write_text_file(*args, **kwargs)

    def create_dir(*args, **kwargs):
        utils.rate_limit_after_n_warnings("Deprecated. Use omigo_hydra")
        return omigo_hydra.s3io_wrapper.create_dir(*args, **kwargs)

    def makedirs(*args, **kwargs):
        utils.rate_limit_after_n_warnings("Deprecated. Use omigo_hydra")
        return omigo_hydra.s3io_wrapper.makedirs(*args, **kwargs)

    def get_last_modified_timestamp(*args, **kwargs):
        utils.rate_limit_after_n_warnings("Deprecated. Use omigo_hydra")
        return omigo_hydra.s3io_wrapper.get_last_modified_timestamp(*args, **kwargs)

    def copy_leaf_dir(*args, **kwargs):
        utils.rate_limit_after_n_warnings("Deprecated. Use omigo_hydra")
        return omigo_hydra.s3io_wrapper.copy_leaf_dir(*args, **kwargs)

    def list_leaf_dir(*args, **kwargs):
        utils.rate_limit_after_n_warnings("Deprecated. Use omigo_hydra")
        return omigo_hydra.s3io_wrapper.list_leaf_dir(*args, **kwargs)


