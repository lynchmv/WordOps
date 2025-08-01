""" Backup and Restore Plugin for WordOps """

import os
import re
import tarfile
import tempfile
import shutil
import subprocess
import pwd
import grp
from datetime import datetime

from cement.core.controller import CementBaseController, expose
from cement.core import handler
from wo.core.logging import Log
from wo.core.util import WOUtil
from wo.core.shellexec import WOShellExec

# --- Configuration ---
BACKUP_BASE_DIR = "/var/www/backups"

def _get_db_creds(wp_config_path):
    """Parses wp-config.php to get database credentials."""
    creds = {}
    try:
        with open(wp_config_path, 'r') as f:
            config_content = f.read()
            # Regex to find define('KEY', 'VALUE');
            patterns = {
                'name': r"define\(\s*'DB_NAME',\s*'([^']+)'\s*\);",
                'user': r"define\(\s*'DB_USER',\s*'([^']+)'\s*\);",
                'password': r"define\(\s*'DB_PASSWORD',\s*'([^']+)'\s*\);",
            }
            for key, pattern in patterns.items():
                match = re.search(pattern, config_content)
                if match:
                    creds[key] = match.group(1)
            return creds if len(creds) == 3 else None
    except IOError:
        return None

def _set_permissions(path, user='www-data', group='www-data'):
    """Recursively sets ownership for a path."""
    try:
        uid = pwd.getpwnam(user).pw_uid
        gid = grp.getgrnam(group).gr_gid
        os.chown(path, uid, gid)
        for root, dirs, files in os.walk(path):
            for d in dirs:
                os.chown(os.path.join(root, d), uid, gid)
            for f in files:
                os.chown(os.path.join(root, f), uid, gid)
        return True
    except (KeyError, OSError) as e:
        Log.debug(f"Could not set permissions: {e}")
        return False

class WOBackupController(CementBaseController):
    """Controller for the `wo backup` command."""
    class Meta:
        label = 'backup'
        stacked_on = 'base'
        stacked_type = 'nested'
        description = 'Backup a site (files, database, and Nginx config)'
        arguments = [
            (['site_name'],
             dict(help='The name of the site to backup', nargs=1)),
        ]

    @expose(hide=True)
    def default(self):
        """The default action when `wo backup` is called"""
        pargs = self.app.pargs
        sitename = pargs.site_name[0]
        site_root = f'/var/www/{sitename}'
        htdocs_path = f'{site_root}/htdocs'
        wp_config_path = f'{htdocs_path}/wp-config.php'
        nginx_config_path = f'/etc/nginx/sites-available/{sitename}'

        if not os.path.isdir(htdocs_path):
            Log.error(self.app, f"Site htdocs not found at {htdocs_path}.")
            return

        Log.info(self.app, f"Starting backup for {sitename}...")

        # 1. Check/Create backup directory
        if not os.path.isdir(BACKUP_BASE_DIR):
            if WOUtil.ask(self.app, f"Backup directory {BACKUP_BASE_DIR} does not exist. Create it now?"):
                try:
                    Log.info(self.app, f"-> Creating backup directory at {BACKUP_BASE_DIR}")
                    os.makedirs(BACKUP_BASE_DIR)
                    if not _set_permissions(BACKUP_BASE_DIR):
                         Log.warn(self.app, f"Could not set permissions on {BACKUP_BASE_DIR}. Please check ownership.")
                except OSError as e:
                    Log.error(self.app, f"Unable to create backup directory: {e}")
                    return
            else:
                Log.info(self.app, "Backup cancelled by user.")
                return

        # 2. Get DB credentials
        Log.info(self.app, "-> Reading database credentials...")
        db_creds = _get_db_creds(wp_config_path)
        if not db_creds:
            Log.error(self.app, f"Could not parse DB credentials from {wp_config_path}")
            return
        Log.info(self.app, f"-> Credentials found for database '{db_creds['name']}'.")

        # 3. Create temp dir
        temp_dir = tempfile.mkdtemp()

        try:
            # 4. Dump database
            db_dump_path = os.path.join(temp_dir, 'database.sql')
            Log.info(self.app, "-> Dumping database...")
            with open(db_dump_path, 'w') as f:
                process = subprocess.run(
                    ['mysqldump', '--no-tablespaces', '-u', db_creds['user'], f"-p{db_creds['password']}", db_creds['name']],
                    stdout=f, stderr=subprocess.PIPE, text=True, check=False
                )
                if process.returncode != 0:
                    Log.error(self.app, f"mysqldump failed: {process.stderr}")
                    return

            # 5. Create archive
            timestamp = datetime.now().strftime("%Y-%m-%d-%H%M%S")
            backup_filename = f"{sitename}-{timestamp}.tar.gz"
            backup_filepath = os.path.join(BACKUP_BASE_DIR, backup_filename)
            Log.info(self.app, "-> Archiving website files, database, and Nginx config...")
            with tarfile.open(backup_filepath, "w:gz") as tar:
                tar.add(htdocs_path, arcname='htdocs')
                tar.add(db_dump_path, arcname='database.sql')
                if os.path.isfile(nginx_config_path):
                    tar.add(nginx_config_path, arcname=f'nginx/{sitename}')
                else:
                    Log.warn(self.app, f"Nginx config not found at {nginx_config_path}, skipping.")


            backup_size = os.path.getsize(backup_filepath) / (1024*1024)
            Log.success(self.app, "Backup complete!")
            Log.info(self.app, f"Backup file created at: {backup_filepath} ({backup_size:.2f} MB)")

        finally:
            # 6. Clean up
            shutil.rmtree(temp_dir)

class WORestoreController(CementBaseController):
    """Controller for the `wo restore` command."""
    class Meta:
        label = 'restore'
        stacked_on = 'base'
        stacked_type = 'nested'
        description = 'Restore a site from a backup'
        arguments = [
            (['site_name'],
             dict(help='The name of the site to restore', nargs=1)),
            (['backup_path'],
             dict(help='Path to the backup .tar.gz file', nargs=1)),
        ]

    @expose(hide=True)
    def default(self):
        """The default action when `wo restore` is called"""
        pargs = self.app.pargs
        sitename = pargs.site_name[0]
        backup_path = pargs.backup_path[0]
        site_root = f'/var/www/{sitename}'
        htdocs_path = f'{site_root}/htdocs'
        nginx_config_path = f'/etc/nginx/sites-available/{sitename}'

        if not os.path.isfile(backup_path):
            Log.error(self.app, f"Backup file not found at {backup_path}")
            return

        # 1. Check if site exists, prompt to create if not
        if not os.path.isdir(site_root):
            if WOUtil.ask(self.app, f"Site {sitename} does not exist. Create a basic HTML site to restore into?"):
                Log.info(self.app, f"Creating placeholder site for {sitename}...")
                try:
                    WOShellExec.cmd_exec(self.app, f"wo site create {sitename} --html")
                except Exception as e:
                    Log.error(self.app, f"Failed to create site: {e}")
                    return
            else:
                Log.info(self.app, "Restore cancelled by user.")
                return

        # Now that site is guaranteed to exist, define wp_config_path
        wp_config_path = f'{htdocs_path}/wp-config.php'

        if not WOUtil.ask(self.app, "This will PERMANENTLY overwrite site files, database, and Nginx config. Are you sure?"):
            Log.info(self.app, "Restore cancelled by user.")
            return

        Log.info(self.app, f"Starting restore for {sitename}...")
        temp_dir = tempfile.mkdtemp()

        try:
            # 2. Extract backup
            Log.info(self.app, "-> Extracting backup file...")
            with tarfile.open(backup_path, "r:gz") as tar:
                tar.extractall(path=temp_dir)

            extracted_htdocs = os.path.join(temp_dir, 'htdocs')
            db_dump_path = os.path.join(temp_dir, 'database.sql')
            extracted_nginx_conf = os.path.join(temp_dir, 'nginx', sitename)

            if not os.path.isdir(extracted_htdocs) or not os.path.isfile(db_dump_path):
                Log.error(self.app, "Backup archive is invalid. Missing htdocs or database.sql.")
                return

            # 3. Get current DB credentials (from the newly created or existing site)
            Log.info(self.app, "-> Reading database credentials...")
            db_creds = _get_db_creds(wp_config_path)
            if not db_creds:
                Log.error(self.app, f"Could not read DB credentials from site at {wp_config_path}")
                return

            # 4. Drop all tables from current database
            Log.info(self.app, f"-> Dropping all tables from database '{db_creds['name']}'...")
            get_tables_sql = "SHOW TABLES;"
            tables_proc = subprocess.run(
                ['mysql', '-u', db_creds['user'], f"-p{db_creds['password']}", '-N', db_creds['name'], '-e', get_tables_sql],
                capture_output=True, text=True, check=False
            )
            if tables_proc.stdout:
                tables = tables_proc.stdout.strip().split('\n')
                drop_sql = "SET FOREIGN_KEY_CHECKS=0; " + "".join([f"DROP TABLE IF EXISTS `{t}`;" for t in tables]) + " SET FOREIGN_KEY_CHECKS=1;"
                drop_proc = subprocess.run(
                    ['mysql', '-u', db_creds['user'], f"-p{db_creds['password']}", db_creds['name'], '-e', drop_sql],
                    capture_output=True, text=True, check=False
                )
                if drop_proc.returncode != 0:
                    Log.error(self.app, f"Failed to drop tables: {drop_proc.stderr}")
                    return

            # 5. Import database from backup
            Log.info(self.app, "-> Importing database from backup...")
            with open(db_dump_path, 'r') as f:
                import_proc = subprocess.run(
                    ['mysql', '-u', db_creds['user'], f"-p{db_creds['password']}", db_creds['name']],
                    stdin=f, capture_output=True, text=True, check=False
                )
                if import_proc.returncode != 0:
                    Log.error(self.app, f"Database import failed: {import_proc.stderr}")
                    return

            # 6. Restore files and Nginx config
            Log.info(self.app, "-> Replacing website files...")
            if os.path.isdir(htdocs_path):
                shutil.rmtree(htdocs_path)
            shutil.move(extracted_htdocs, site_root)

            if os.path.isfile(extracted_nginx_conf):
                Log.info(self.app, "-> Restoring Nginx configuration...")
                shutil.copy(extracted_nginx_conf, nginx_config_path)

            # 7. Set permissions
            Log.info(self.app, "-> Setting file and directory permissions...")
            if not _set_permissions(htdocs_path):
                Log.warn(self.app, "Could not set www-data permissions. Please set them manually.")

            # 8. Reload Nginx
            Log.info(self.app, "-> Reloading Nginx stack...")
            WOShellExec.cmd_exec(self.app, "wo stack reload --nginx")

            Log.success(self.app, "Restore complete!")

        finally:
            # 9. Clean up
            shutil.rmtree(temp_dir)

def load(app):
    """Loads the plugin controllers into the WordOps application"""
    handler.register(WOBackupController)
    handler.register(WORestoreController)
