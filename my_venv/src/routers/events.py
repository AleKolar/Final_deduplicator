from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession

from my_venv.src.database.database import get_db
from my_venv.src.models.ORM_models import EventIncomingORM
from my_venv.src.models.pydentic_models import EventResponse, EventCreate

router = APIRouter()

@router.get("/")
async def root():
    return {"message": "Hello World"}


from sqlalchemy.exc import IntegrityError
from fastapi import HTTPException, status


@router.post("/post", response_model=EventResponse)
async def create_event(
        event_data: EventCreate,
        session: AsyncSession = Depends(get_db)
):
    try:
        db_event = EventIncomingORM(**event_data.model_dump())

        session.add(db_event)
        await session.commit()
        await session.refresh(db_event)

        return EventResponse.model_validate(db_event)

    except IntegrityError as e:
        await session.rollback()
        if "unique constraint" in str(e).lower() and "event_hash" in str(e).lower():
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail="Event hash already exists"
            )
        # Логируем полную ошибку для диагностики
        print(f"IntegrityError details: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Database integrity error"
        )

    except Exception as e:
        await session.rollback()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Internal server error: {str(e)}"
        )