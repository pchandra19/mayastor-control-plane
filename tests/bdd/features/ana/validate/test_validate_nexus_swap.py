"""Swap ANA enabled Nexus on ANA enabled host feature tests."""

from urllib.parse import urlparse

from pytest_bdd import (
    given,
    scenario,
    then,
    when,
)
import pytest
import subprocess

from common.deployer import Deployer
from common.apiclient import ApiClient
from common.docker import Docker
from common.fio import Fio
from common.nvme import (
    nvme_connect,
    nvme_disconnect,
    nvme_list_subsystems,
    nvme_disconnect_controller,
)

from openapi.model.create_pool_body import CreatePoolBody
from openapi.model.create_volume_body import CreateVolumeBody
from openapi.model.publish_volume_body import PublishVolumeBody
from openapi.model.volume_policy import VolumePolicy
from openapi.model.volume_share_protocol import VolumeShareProtocol

VOLUME_UUID = "5cd5378e-3f05-47f1-a830-a0f5873a1449"
VOLUME_SIZE = 10485761
POOL_UUID = "4cc6ee64-7232-497d-a26f-38284a444980"
POOL_NODE = "io-engine-3"
TARGET_NODE_1 = "io-engine-1"
TARGET_NODE_2 = "io-engine-2"
FIO_RUNTIME = 4


@scenario(
    "validate-nexus-swap.feature",
    "replace failed I/O path on demand for NVMe controller",
)
def test_replace_failed_io_path_on_demand_for_nvme_controller():
    """replace failed I/O path on demand for NVMe controller."""


@given("a client connected to one nexus via single I/O path")
def a_client_connected_to_one_nexus_via_single_io_path(connect_to_first_path):
    """a client connected to one nexus via single I/O path."""
    pass


@given(
    "a control plane, 2 ANA-enabled Io-Engine instances, 1 ANA-enabled host and a published volume"
)
def a_control_plane_2_anaenabled_io_engine_instances_1_anaenabled_host_and_a_published_volume(
    background,
):
    """a control plane, 2 ANA-enabled Io-Engine instances, 1 ANA-enabled host and a published volume."""
    volume = background
    assert hasattr(volume.state, "target")
    pass


@given("fio client is running against target nexus")
def fio_client_is_running_against_target_nexus(run_fio_to_first_path):
    """fio client is running against target nexus."""
    pass


@when("the only I/O path degrades")
def the_only_io_path_degrades(degrade_first_path):
    """the only I/O path degrades."""
    pass


@then("fio client should successfully complete with the replaced I/O path")
def fio_client_should_successfully_complete_with_the_replaced_io_path(
    fio_completes_successfully,
):
    """fio client should successfully complete with the replaced I/O path."""
    pass


@then(
    "it should be possible to create a second nexus and connect it as the second path"
)
def it_should_be_possible_to_create_a_second_nexus_and_connect_it_as_the_second_path(
    publish_to_node_2, connect_to_node_2
):
    """it should be possible to create a second nexus and connect it as the second path."""
    pass


@then("it should be possible to remove the first failed I/O path")
def it_should_be_possible_to_remove_the_first_failed_io_path(remove_first_path):
    """it should be possible to remove the first failed I/O path."""
    pass


"""" FixTure Implementations """


@pytest.fixture
def background():
    Deployer.start(
        3,
        cache_period="1s",
        io_engine_coreisol=True,
        node_conn_timeout="100ms",
    )

    ApiClient.pools_api().put_node_pool(
        POOL_NODE, POOL_UUID, CreatePoolBody(["malloc:///disk?size_mb=100"])
    )
    ApiClient.volumes_api().put_volume(
        VOLUME_UUID, CreateVolumeBody(VolumePolicy(False), 1, VOLUME_SIZE, False)
    )
    volume = ApiClient.volumes_api().put_volume_target(
        VOLUME_UUID,
        publish_volume_body=PublishVolumeBody(
            {}, VolumeShareProtocol("nvmf"), node=TARGET_NODE_1
        ),
    )
    yield volume
    Deployer.stop()


@pytest.fixture
def connect_to_first_path(background):
    volume = background
    print(volume)
    print(volume.state["target"])
    device_uri = volume.state["target"]["device_uri"]
    yield nvme_connect(device_uri)
    nvme_disconnect(device_uri)


@pytest.fixture
def run_fio_to_first_path(connect_to_first_path):
    device = connect_to_first_path
    desc = nvme_list_subsystems(device)
    assert (
        len(desc["Subsystems"]) == 1
    ), "Must be exactly one NVMe subsystem for target nexus"
    subsystem = desc["Subsystems"][0]
    assert len(subsystem["Paths"]) == 1, "Must be exactly one I/O path to target nexus"
    assert subsystem["Paths"][0]["State"] == "live", "I/O path is not healthy"
    # Launch fio in background and let it always run along with the test.
    fio = Fio("job", "randread", device, runtime=FIO_RUNTIME)
    return fio.open()


@pytest.fixture
def degrade_first_path():
    Docker.kill_container(TARGET_NODE_1)


@pytest.fixture
def publish_to_node_2(background):
    volume = background
    device_uri = volume.state["target"]["device_uri"]

    volume_updated = ApiClient.volumes_api().put_volume_target(
        VOLUME_UUID,
        publish_volume_body=PublishVolumeBody(
            {},
            VolumeShareProtocol("nvmf"),
            node=TARGET_NODE_2,
            reuse_existing=False,
            republish=True,
            force=True,
        ),
    )
    device_uri_2 = volume_updated.state["target"]["device_uri"]
    assert device_uri != device_uri_2
    return device_uri_2


@pytest.fixture
def connect_to_node_2(publish_to_node_2):
    device = nvme_connect(publish_to_node_2)
    desc = nvme_list_subsystems(device)
    subsystem = desc["Subsystems"][0]
    assert len(subsystem["Paths"]) == 2, "Second nexus must be added to I/O path"

    good_path_checked = False
    broken_path_checked = False
    uri = urlparse(publish_to_node_2)
    for p in subsystem["Paths"]:
        if uri.hostname not in p["Address"]:
            assert p["State"] == "connecting", "Degraded I/O path has incorrect state"
            broken_path_checked = True
        else:
            assert p["State"] == "live", f"Healthy I/O path has incorrect state"
            good_path_checked = True
    assert good_path_checked, "No state reported for healthy I/O path"
    assert broken_path_checked, "No state reported for broken I/O path"


@pytest.fixture
def remove_first_path(connect_to_first_path):
    device_1 = connect_to_first_path
    desc = nvme_list_subsystems(device_1)
    # Find the name of the failed controller and disconnect it.
    broken_ctrlrs = [
        p["Name"] for p in desc["Subsystems"][0]["Paths"] if p["State"] == "connecting"
    ]
    assert len(broken_ctrlrs) == 1, "No degraded paths reported"
    nvme_disconnect_controller(broken_ctrlrs[0])

    # Check that there is only 1 healthy path left.
    desc = nvme_list_subsystems(device_1)
    subsystem = desc["Subsystems"][0]
    assert len(subsystem["Paths"]) == 1, "Insufficient number of I/O paths reported"
    assert subsystem["Paths"][0]["State"] == "live", "No healthy path reported"


@pytest.fixture
def fio_completes_successfully(run_fio_to_first_path):
    try:
        code = run_fio_to_first_path.wait(timeout=FIO_RUNTIME * 2)
    except subprocess.TimeoutExpired:
        assert False, "FIO timed out"
    assert code == 0, "FIO failed, exit code: %d" % code
