import importlib.util
import sys
from importlib.machinery import EXTENSION_SUFFIXES
from pathlib import Path
from typing import Generator

_CAPNP_MODULE_NAME = f"{__name__}.capnp"


def __iter_capnp_module_paths() -> Generator[Path, None, None]:
    seen_paths = set()
    package_paths = [Path(__file__).resolve().parent]
    package_paths.extend(Path(sys_path).resolve() / "scaler" / "protocol" for sys_path in sys.path if sys_path)

    for package_path in package_paths:
        for extension_suffix in EXTENSION_SUFFIXES:
            module_path = package_path / f"capnp{extension_suffix}"

            if module_path in seen_paths or not module_path.is_file():
                continue

            seen_paths.add(module_path)
            yield module_path


capnp = sys.modules.get(_CAPNP_MODULE_NAME)
if capnp is None:
    import_error = None
    for capnp_module_path in __iter_capnp_module_paths():
        capnp_spec = importlib.util.spec_from_file_location(_CAPNP_MODULE_NAME, capnp_module_path)
        if capnp_spec is None or capnp_spec.loader is None:
            continue

        try:
            capnp = importlib.util.module_from_spec(capnp_spec)
            sys.modules[_CAPNP_MODULE_NAME] = capnp
            capnp_spec.loader.exec_module(capnp)
            break
        except (ImportError, TypeError) as error:
            sys.modules.pop(_CAPNP_MODULE_NAME, None)
            import_error = error
    else:
        raise import_error or ImportError(f"cannot load {_CAPNP_MODULE_NAME} from any known module path")
