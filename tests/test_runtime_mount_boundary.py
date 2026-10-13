from types import SimpleNamespace

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
