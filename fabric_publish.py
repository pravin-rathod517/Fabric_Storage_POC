from fabric_cicd import FabricWorkspace, publish_all_items, unpublish_all_orphan_items
import os

# Pull credentials and config from environment variables
workspace_id = os.getenv("WORKSPACE_ID")
repo_dir = os.getenv("FABRIC_REPO_DIR", ".")
item_types = os.getenv("FABRIC_ITEM_TYPES", "Notebook,DataPipeline,Environment").split(",")

if not workspace_id:
    raise ValueError("WORKSPACE_ID environment variable is required!")

# Initialize FabricWorkspace
target_workspace = FabricWorkspace(
    workspace_id=workspace_id,
    repository_directory=repo_dir,
    item_type_in_scope=item_types,
)


# Publish items
publish_all_items(target_workspace)

# Unpublish orphan items
unpublish_all_orphan_items(target_workspace)
 
