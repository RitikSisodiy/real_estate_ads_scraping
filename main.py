import uvicorn
from fastapi import FastAPI, HTTPException
from starlette.middleware.cors import CORSMiddleware
from api import router as real_estate_router
from settings import API_V1_STR, DEBUG, title

"""---------------------------------------- Application Settings  ---------------------------------------- """

app = FastAPI(title=title, openapi_url="/api/v1/openapi.json", debug=DEBUG)

# CORS
origins = []
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
),
# ROUTES
app.include_router(real_estate_router, prefix=API_V1_STR)
