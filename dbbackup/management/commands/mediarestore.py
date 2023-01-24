"""
Restore media files.
"""
import tarfile
from pathlib import Path

from django.core.files.storage import Storage
from typing import Dict

from ... import utils
from ...storage import get_storage
from ._base import BaseDbBackupCommand, make_option

from dbbackup import settings


class Command(BaseDbBackupCommand):
    help = """Restore a media backup from storage, encrypted and/or
    compressed."""
    content_type = "media"
    content_storages: Dict[str, Storage] = settings.BACKUP_LOCATIONS

    option_list = (
        make_option(
            "-i",
            "--input-filename",
            action="store",
            help="Specify filename to backup from",
        ),
        make_option(
            "-I", "--input-path", help="Specify path on local filesystem to backup from"
        ),
        make_option(
            "-s",
            "--servername",
            help="If backup file is not specified, filter the existing ones with the "
            "given servername",
        ),
        make_option(
            "-e",
            "--decrypt",
            default=False,
            action="store_true",
            help="Decrypt data before restoring",
        ),
        make_option(
            "-p", "--passphrase", default=None, help="Passphrase for decrypt file"
        ),
        make_option(
            "-z",
            "--uncompress",
            action="store_true",
            help="Uncompress gzip data before restoring",
        ),
        make_option(
            "-r", "--replace", help="Replace existing files", action="store_true"
        ),
    )

    def handle(self, *args, **options):
        """Django command handler."""
        self.verbosity = int(options.get("verbosity"))
        self.quiet = options.get("quiet")
        self._set_logger_level()

        self.servername = options.get("servername")
        self.decrypt = options.get("decrypt")
        self.uncompress = options.get("uncompress")

        self.filename = options.get("input_filename")
        self.path = options.get("input_path")

        self.replace = options.get("replace")
        self.passphrase = options.get("passphrase")
        self.interactive = options.get("interactive")

        self.storage = get_storage()
        self._restore_backup()

    def _upload_file(self, name, content_storage, content_file):
        if content_storage.exists(name):
            if not self.replace:
                return
            content_storage.delete(name)
            self.logger.info("%s deleted from %s", name, content_storage)
        content_storage.save(name, content_file)
        self.logger.info("%s uploaded to %s", name, content_storage)

    def _restore_backup(self):
        self.logger.info(f"Restoring backup for {self.content_type} files")
        input_filename, input_file = self._get_backup_file(servername=self.servername)
        self.logger.info("Restoring: %s", input_filename)

        if self.decrypt:
            unencrypted_file, input_filename = utils.unencrypt_file(
                input_file, input_filename, self.passphrase
            )
            input_file.close()
            input_file = unencrypted_file

        self.logger.debug("Backup size: %s", utils.handle_size(input_file))
        if self.interactive:
            self._ask_confirmation()

        input_file.seek(0)
        tar_file = (
            tarfile.open(fileobj=input_file, mode="r:gz")
            if self.uncompress
            else tarfile.open(fileobj=input_file, mode="r:")
        )
        # Restore file 1 by 1
        for file_info in tar_file:
            p = Path(file_info.path)
            content_storage_label = p.parts[0]
            content_storage = self.content_storages[content_storage_label]
            self.logger.info("extracting %s for %s", p, content_storage_label)
            # if file_info.path == self.content_type:
            #     continue  # Don't copy root directory
            content_file = tar_file.extractfile(file_info)
            if content_file is None:
                continue  # Skip directories
            name = str(Path(*p.parts[1:]))
            self._upload_file(name, content_storage, content_file)
