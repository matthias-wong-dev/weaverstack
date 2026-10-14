from types import SimpleNamespace

import pytest
from support.weaver_test import weaver_test

from weaver.lakehouse import Lakehouse, release_mounts


@weaver_test()
def test_a_released_mount_path_is_never_reused(monkeypatch):
    import weaver.lakehouse as module

    mounted = []
    unmounted = []

    class Fs:
        def mount(self, root, point, options):
            mounted.append(point)

        def getMountPath(self, point):
            return "/synfs/session" + point

        def unmount(self, point):
            unmounted.append(point)

    monkeypatch.setattr(module, "_notebook_utils", lambda: SimpleNamespace(fs=Fs()))
    monkeypatch.setattr(module, "_MOUNTS", {})
    lakehouse = Lakehouse(name="Sales_LH", spark_root="abfss://ws@host/lh")
    before = lakehouse.files_root()
    assert lakehouse.files_root() == before
    release_mounts()
    after = lakehouse.files_root()
    assert after != before
    assert len(mounted) == 2
    assert unmounted == mounted[:1]


class _Fs:
    def __init__(
        self, *, path=None, mount_error=None, path_error=None, unmount_error=None
    ):
        self.path = path
        self.mount_error = mount_error
        self.path_error = path_error
        self.unmount_error = unmount_error
        self.mounted = []
        self.unmounted = []

    def mount(self, root, point, options):
        if self.mount_error:
            raise self.mount_error
        self.mounted.append(point)

    def getMountPath(self, point):
        if self.path_error:
            raise self.path_error
        return self.path if self.path is not None else "/synfs/session" + point

    def unmount(self, point):
        self.unmounted.append(point)
        if self.unmount_error:
            raise self.unmount_error


def _mounting(monkeypatch, fs):
    import weaver.lakehouse as module

    monkeypatch.setattr(module, "_notebook_utils", lambda: SimpleNamespace(fs=fs))
    monkeypatch.setattr(module, "_MOUNTS", {})
    return module


@weaver_test()
def test_a_successful_mount_is_registered_and_reused(monkeypatch):
    fs = _Fs()
    module = _mounting(monkeypatch, fs)
    lakehouse = Lakehouse(name="Sales_LH", spark_root="abfss://ws@host/lh")
    first = lakehouse.files_root()
    assert lakehouse.files_root() == first
    assert len(fs.mounted) == 1 and not fs.unmounted
    assert module._MOUNTS["abfss://ws@host/lh"].point == fs.mounted[0]


@weaver_test()
def test_a_failed_mount_path_lookup_unmounts_and_registers_nothing(monkeypatch):
    import pytest

    from weaver.errors import LoadError

    fs = _Fs(path_error=RuntimeError("no path"))
    module = _mounting(monkeypatch, fs)
    with pytest.raises(LoadError, match="no path") as raised:
        Lakehouse(name="Sales_LH", spark_root="abfss://ws@host/lh").files_root()
    assert isinstance(raised.value.__cause__, RuntimeError)
    assert fs.unmounted == fs.mounted and len(fs.mounted) == 1
    assert not module._MOUNTS


@weaver_test()
@pytest.mark.parametrize("returned", ["", "  ", None])
def test_an_unusable_mount_path_unmounts_and_registers_nothing(monkeypatch, returned):
    import pytest

    from weaver.errors import LoadError

    fs = _Fs()
    fs.getMountPath = lambda point: returned
    module = _mounting(monkeypatch, fs)
    with pytest.raises(LoadError, match="no filesystem path"):
        Lakehouse(name="Sales_LH", spark_root="abfss://ws@host/lh").files_root()
    assert fs.unmounted == fs.mounted and len(fs.mounted) == 1
    assert not module._MOUNTS


@weaver_test()
def test_a_cleanup_failure_leaves_the_original_error_primary(monkeypatch):
    import pytest

    from weaver.errors import LoadError

    fs = _Fs(path_error=RuntimeError("no path"), unmount_error=OSError("busy"))
    _mounting(monkeypatch, fs)
    with pytest.raises(LoadError, match="no path"):
        Lakehouse(name="Sales_LH", spark_root="abfss://ws@host/lh").files_root()
    assert len(fs.unmounted) == 1


@weaver_test()
def test_a_failed_mount_has_nothing_to_unmount(monkeypatch):
    import pytest

    from weaver.errors import LoadError

    fs = _Fs(mount_error=RuntimeError("refused"))
    module = _mounting(monkeypatch, fs)
    with pytest.raises(LoadError, match="refused"):
        Lakehouse(name="Sales_LH", spark_root="abfss://ws@host/lh").files_root()
    assert not fs.unmounted and not module._MOUNTS
