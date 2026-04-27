import logging
from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.core.config import settings
from app.core.database import db
from app.services.token_manager import token_manager
from app.services.data_collector import data_collector
from app.services.candle_aggregator import candle_aggregator
from app.api import auth, admin, tokens, stream, candles
from app.scripts.seed_admin import seed_initial_admin, seed_initial_token

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan manager"""
    logger.info("🚀 Starting Live Data Engine...")
    
    # Connect to databases
    await db.connect()
    
    # Seed initial admin and token
    await seed_initial_admin()
    await seed_initial_token()
    
    # Initialize token manager
    await token_manager.initialize()
    
    # Start data collection
    await data_collector.start()
    
    # Start candle aggregation
    await candle_aggregator.start()
    
    logger.info("✅ Live Data Engine started successfully")
    
    yield
    
    # Shutdown
    logger.info("⏹️  Shutting down Live Data Engine...")
    await candle_aggregator.stop()
    await data_collector.stop()
    await db.disconnect()
    logger.info("✅ Shutdown complete")


# Create FastAPI app
app = FastAPI(
    title="Live Data Engine",
    description="Multi-token data collection engine for Tiingo API",
    version="1.0.0",
    lifespan=lifespan,
)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.allowed_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include routers
app.include_router(auth.router, prefix="/api")
app.include_router(admin.router, prefix="/api")
app.include_router(tokens.router, prefix="/api")
app.include_router(stream.router, prefix="/api")
app.include_router(candles.router, prefix="/api")


@app.get("/")
async def root():
    """Health check endpoint"""
    return {
        "status": "running",
        "service": "Live Data Engine",
        "version": "1.0.0",
    }


@app.get("/health")
async def health_check():
    """Detailed health check"""
    tokens = await token_manager.get_all_tokens()
    stats = await token_manager.get_token_stats()
    
    return {
        "status": "healthy",
        "database": {
            "postgresql": "connected" if db.pool else "disconnected",
        },
        "tokens": {
            "total": len(tokens),
            "active": len([t for t in tokens if t.status == "active"]),
            "healthy": len([s for s in stats if s.is_healthy]),
        },
        "collection": {
            "bitcoin": settings.COLLECT_BTC,
            "gold": settings.COLLECT_GOLD,
            "silver": settings.COLLECT_SILVER,
        },
    }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "app.main:app",
        host="0.0.0.0",
        port=settings.PORT,
        reload=settings.NODE_ENV == "development",
    )
