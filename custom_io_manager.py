import os
import shutil
from dagster import (
    Field,
    StringSource,
    IOManager,
    check,
    OutputContext,
    InputContext,
    AssetMaterialization,
    EventMetadataEntry,
    AssetKey,
    io_manager,
)
from dagster_azure.adls2.utils import ResourceNotFoundError
from dagster.utils import mkdir_p


_LEASE_DURATION = 60  # One minute


class CustomFilePathADLS2IOManager(IOManager):
    """Dagster IO manager that saves result to Azure ADLS2 at provided path"""

    def __init__(self, adls2_client, file_system, base_dir="/output", prefix=None):
        self.adls2_client = adls2_client
        self.file_system_client = self.adls2_client.get_file_system_client(file_system)
        self.lease_duration = _LEASE_DURATION
        self.base_dir = check.str_param(base_dir, "base_dir")
        self.prefix = check.opt_str_param(prefix, "prefix")

    def _get_path(self, context):
        run_id, _, _ = context.get_run_scoped_output_identifier()
        if self.prefix:
            return "/".join([self.prefix, run_id, context.config["output_path"]])
        else:
            return "/".join([run_id, context.config["output_path"]])

    def _get_local_path(self, context):
        run_id, _, _ = context.get_run_scoped_output_identifier()
        return os.path.join(self.base_dir, run_id, context.config["output_path"])

    def _rm_object(self, key):
        check.str_param(key, "key")
        check.param_invariant(len(key) > 0, "key")

        # This operates recursively already so is nice and simple.
        self.file_system_client.delete_file(key)

    def _has_object(self, key):
        check.str_param(key, "key")
        check.param_invariant(len(key) > 0, "key")

        try:
            file = self.file_system_client.get_file_client(key)
            file.get_file_properties()
            return True
        except ResourceNotFoundError:
            return False

    def _uri_for_key(self, key, protocol=None):
        check.str_param(key, "key")
        protocol = check.opt_str_param(protocol, "protocol", default="abfss://")
        return "{protocol}{filesystem}@{account}.dfs.core.windows.net/{key}".format(
            protocol=protocol,
            filesystem=self.file_system_client.file_system_name,
            account=self.file_system_client.account_name,
            key=key,
        )

    def handle_output(self, context, obj):
        """Write the provided file to ADLS at the custom path provided"""
        check.inst_param(context, "context", OutputContext)
        key = self._get_path(context)
        context.log.info(f"Writing ADLS2 object at: {self._uri_for_key(key)}")

        if self._has_object(key):
            context.log.warning(f"Removing existing ADLS2 key: {key}")
            self._rm_object(key)

        file = self.file_system_client.create_file(key)
        with file.acquire_lease(self.lease_duration) as lease, open(
            obj, mode="rb"
        ) as input_file:
            file.upload_data(input_file, lease=lease, overwrite=True)

        return AssetMaterialization(
            asset_key=AssetKey([context.pipeline_name, context.step_key, context.name]),
            metadata_entries=[EventMetadataEntry.fspath(self._uri_for_key(key))],
        )

    def load_input(self, context):
        """Read the file from the ADLS2 path and write it to local storage to be used by the solid"""
        check.inst_param(context, "context", InputContext)
        key = self._get_path(context.upstream_output)
        context.log.info(f"Loading ADLS2 object from: {self._uri_for_key(key)}")
        file = self.file_system_client.get_file_client(key)
        local_filepath = self._get_local_path(context.upstream_output)
        mkdir_p(os.path.dirname(local_filepath))
        with open(local_filepath, mode="wb") as fp:
            fp.write(file.download_file().readall())
        return local_filepath


@io_manager(
    config_schema={
        "adls2_file_system": Field(
            StringSource, description="ADLS Gen2 file system name"
        ),
        "base_dir": Field(StringSource, description="Base dir to save temporary files"),
        "prefix": Field(
            StringSource,
            is_required=False,
            description="Prefix to use for all ADLS filepath",
        ),
    },
    required_resource_keys={"adls2"},
    output_config_schema={"output_path": str},
)
def adls2_file_io_manager(init_context):
    adls_resource = init_context.resources.adls2
    adls2_client = adls_resource.adls2_client
    adls_io_manager = CustomFilePathADLS2IOManager(
        adls2_client=adls2_client,
        file_system=init_context.resource_config["adls2_file_system"],
        base_dir=init_context.resource_config["base_dir"],
        prefix=init_context.resource_config.get("prefix"),
    )
    return adls_io_manager


class CustomFilePathOManager(IOManager):
    """Dagster IO manager that saves result to local storage at provided path"""

    def __init__(self, base_dir="/output"):
        self.base_dir = check.str_param(base_dir, "base_dir")

    def _get_path(self, context):
        run_id, _, _ = context.get_run_scoped_output_identifier()
        return os.path.join(self.base_dir, run_id, context.config["output_path"])

    def handle_output(self, context, obj):
        """Write the provided file to the custom filepath"""
        check.inst_param(context, "context", OutputContext)
        filepath = self._get_path(context)

        # Ensure path exists
        mkdir_p(os.path.dirname(filepath))
        context.log.info(f"Writing file at: {filepath}")

        with open(filepath, mode="wb") as write_obj, open(obj, mode="rb") as read_obj:
            shutil.copyfileobj(read_obj, write_obj)

        return AssetMaterialization(
            asset_key=AssetKey([context.pipeline_name, context.step_key, context.name]),
            metadata_entries=[EventMetadataEntry.fspath(os.path.abspath(filepath))],
        )

    def load_input(self, context):
        """Get the custom filepath from the previous solid and provide the path directly for use"""
        check.inst_param(context, "context", InputContext)
        filepath = self._get_path(context.upstream_output)
        context.log.info(f"Return filepath from: {filepath}")

        return filepath


@io_manager(
    config_schema={
        "base_dir": Field(StringSource, description="Local file path to save files"),
    },
    output_config_schema={"output_path": str},
)
def filepath_io_manager(init_context):
    return CustomFilePathOManager(
        base_dir=init_context.resource_config["base_dir"],
    )
