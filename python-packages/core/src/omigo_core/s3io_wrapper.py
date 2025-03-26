from omigo_hydra import s3io_wrapper
from omigo_core import utils

# Deprecated. Will be removed soon
class S3FSWrapper:
    def __init__(self):
        self.fs = s3io_wrapper.S3FSWrapper()

    def file_exists(self, *args, **kwargs):
        utils.rate_limit_after_n_warnings("Deprecated. Use omigo_hydra")
        return self.fs.file_exists(*args, **kwargs)

    def file_exists_with_wait(self, *args, **kwargs):
        utils.rate_limit_after_n_warnings("Deprecated. Use omigo_hydra")
        return self.fs.file_exists_with_wait(*args, **kwargs)

    def dir_exists(self, *args, **kwargs):
        utils.rate_limit_after_n_warnings("Deprecated. Use omigo_hydra")
        return self.fs.dir_exists(*args, **kwargs)

    def dir_exists_with_wait(self, *args, **kwargs):
        utils.rate_limit_after_n_warnings("Deprecated. Use omigo_hydra")
        return self.fs.dir_exists_with_wait(*args, **kwargs)

    def file_not_exists(self, *args, **kwargs):
        utils.rate_limit_after_n_warnings("Deprecated. Use omigo_hydra")
        return self.fs.file_not_exists(*args, **kwargs)

    def file_not_exists_with_wait(self, *args, **kwargs):
        utils.rate_limit_after_n_warnings("Deprecated. Use omigo_hydra")
        return self.fs.file_not_exists_with_wait(*args, **kwargs)

    def dir_not_exists_with_wait(self, *args, **kwargs):
        utils.rate_limit_after_n_warnings("Deprecated. Use omigo_hydra")
        return self.fs.dir_not_exists_with_wait(*args, **kwargs)

    def read_file_contents_as_text_with_wait(self, *args, **kwargs):
        utils.rate_limit_after_n_warnings("Deprecated. Use omigo_hydra")
        return self.fs.read_file_contents_as_text_with_wait(*args, **kwargs)

    def read_file_contents_as_text(self, *args, **kwargs):
        utils.rate_limit_after_n_warnings("Deprecated. Use omigo_hydra")
        return self.fs.read_file_contents_as_text(*args, **kwargs)

    def ls(self, *args, **kwargs):
        utils.rate_limit_after_n_warnings("Deprecated. Use omigo_hydra")
        return self.fs.ls(*args, **kwargs)

    def get_directory_listing(self, *args, **kwargs):
        utils.rate_limit_after_n_warnings("Deprecated. Use omigo_hydra")
        return self.fs.get_directory_listing(*args, **kwargs)

    def list_dirs(self, *args, **kwargs):
        utils.rate_limit_after_n_warnings("Deprecated. Use omigo_hydra")
        return self.fs.list_dirs(*args, **kwargs)

    def list_files(self, *args, **kwargs):
        utils.rate_limit_after_n_warnings("Deprecated. Use omigo_hydra")
        return self.fs.list_files(*args, **kwargs)

    def is_file(self, *args, **kwargs):
        utils.rate_limit_after_n_warnings("Deprecated. Use omigo_hydra")
        return self.fs.is_file(*args, **kwargs)

    def is_directory(self, *args, **kwargs):
        utils.rate_limit_after_n_warnings("Deprecated. Use omigo_hydra")
        return self.fs.is_directory(*args, **kwargs)

    def delete_file_with_wait(self, *args, **kwargs):
        utils.rate_limit_after_n_warnings("Deprecated. Use omigo_hydra")
        return self.fs.delete_file_with_wait(*args, **kwargs)

    def delete_file(self, *args, **kwargs):
        utils.rate_limit_after_n_warnings("Deprecated. Use omigo_hydra")
        return self.fs.delete_file(*args, **kwargs)

    def delete_dir_with_wait(self, *args, **kwargs):
        utils.rate_limit_after_n_warnings("Deprecated. Use omigo_hydra")
        return self.fs.delete_dir_with_wait(*args, **kwargs)

    def get_parent_directory(self, *args, **kwargs):
        utils.rate_limit_after_n_warnings("Deprecated. Use omigo_hydra")
        return self.fs.get_parent_directory(*args, **kwargs)

    def write_text_file(self, *args, **kwargs):
        utils.rate_limit_after_n_warnings("Deprecated. Use omigo_hydra")
        return self.fs.write_text_file(*args, **kwargs)

    def create_dir(self, *args, **kwargs):
        utils.rate_limit_after_n_warnings("Deprecated. Use omigo_hydra")
        return self.fs.create_dir(*args, **kwargs)

    def makedirs(self, *args, **kwargs):
        utils.rate_limit_after_n_warnings("Deprecated. Use omigo_hydra")
        return self.fs.makedirs(*args, **kwargs)

    def get_last_modified_timestamp(self, *args, **kwargs):
        utils.rate_limit_after_n_warnings("Deprecated. Use omigo_hydra")
        return self.fs.get_last_modified_timestamp(*args, **kwargs)

    def copy_leaf_dir(self, *args, **kwargs):
        utils.rate_limit_after_n_warnings("Deprecated. Use omigo_hydra")
        return self.fs.copy_leaf_dir(*args, **kwargs)

    def list_leaf_dir(self, *args, **kwargs):
        utils.rate_limit_after_n_warnings("Deprecated. Use omigo_hydra")
        return self.fs.list_leaf_dir(*args, **kwargs)


