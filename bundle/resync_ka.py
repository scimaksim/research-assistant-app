"""Nudge the Knowledge Assistant to re-index after PDFs were added/removed.

Looks up the KA by display name and issues a sync on its knowledge source.
"""
import argparse
import sys
import time

from databricks.sdk import WorkspaceClient


DISPLAY_NAME_KA = "Research Assistant Knowledge"


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--catalog", required=True)
    p.add_argument("--schema", required=True)
    p.add_argument("--volume", required=True)
    args = p.parse_args()

    w = WorkspaceClient()
    ka = next(
        (k for k in w.knowledge_assistants.list_knowledge_assistants() if k.display_name == DISPLAY_NAME_KA),
        None,
    )
    if not ka:
        print(f"[resync_ka] KA '{DISPLAY_NAME_KA}' not found — run bootstrap first.", file=sys.stderr)
        sys.exit(1)
    parent = f"knowledge-assistants/{ka.id}"
    for src in w.knowledge_assistants.list_knowledge_sources(parent=parent):
        print(f"[resync_ka] syncing {src.name} (state={src.state}) ...")
        try:
            w.knowledge_assistants.sync_knowledge_source(name=src.name)
        except AttributeError:
            # Older SDKs — update with same payload to trigger re-index
            from databricks.sdk.service.agents import KnowledgeSource, FilesSpec
            vol_path = f"/Volumes/{args.catalog}/{args.schema}/{args.volume}"
            w.knowledge_assistants.update_knowledge_source(
                name=src.name,
                knowledge_source=KnowledgeSource(
                    display_name=src.display_name,
                    source_type="files",
                    files=FilesSpec(path=vol_path),
                ),
                update_mask="files",
            )
    # Poll until UPDATED
    for _ in range(60):
        all_updated = True
        for src in w.knowledge_assistants.list_knowledge_sources(parent=parent):
            if str(src.state) != "KnowledgeSourceState.UPDATED":
                all_updated = False
                print(f"[resync_ka]   {src.display_name} state={src.state}")
        if all_updated:
            print("[resync_ka] done")
            return
        time.sleep(15)
    print("[resync_ka] sync still in progress; check the KA page in the workspace.")


if __name__ == "__main__":
    main()
