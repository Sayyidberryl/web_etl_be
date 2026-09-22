import os
from api.index import app, safe_int

if __name__ == "__main__":
    import uvicorn
    port = safe_int(os.getenv("PORT"), 8000)
    uvicorn.run("main:app", host="0.0.0.0", port=port, reload=True)
