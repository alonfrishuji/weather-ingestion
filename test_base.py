from server.database import Base as DatabaseBase
from server.models import Base as ModelsBase

print("Base in server.database:", DatabaseBase)
print("Base in server.models:", ModelsBase)
assert DatabaseBase is ModelsBase, "Base objects do not match!"
