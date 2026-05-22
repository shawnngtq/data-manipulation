import os
import shutil


def on_pre_build(config, **kwargs):
    root = os.path.dirname(config["docs_dir"])
    shutil.copy(
        os.path.join(root, "CHANGELOG.md"),
        os.path.join(config["docs_dir"], "changelog.md"),
    )
