from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from config import DATABASE_URL
from db_consumer import DbConsumer
from models import Base, Product

if __name__ == '__main__':
    engine = create_engine(DATABASE_URL)
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)

    SessionLocal = sessionmaker(bind=engine, autocommit=False, autoflush=False)

    #region data population
    p1 = Product(
        name='laptop',
        description= 'test descr for laptop',
        selling_price= 999.99,
        cost_price= 800,
        amount= 25
    )

    p2 = Product(
        name='wireless_mouse',
        description='test descr for wireless_mouse',
        selling_price=19.99,
        cost_price=15.99,
        amount=100
    )

    p3 = Product(
        name='mechanical_keyboard',
        description='test descr for mechanical_keyboard',
        selling_price=89.99,
        cost_price=70.99,
        amount=50
    )

    p4 = Product(
        name='usb_c_charger',
        description='test descr for usb_c_charger',
        selling_price=24.49,
        cost_price=15.30,
        amount=80
    )

    p5 = Product(
        name='smartphone',
        description='test descr for smartphone',
        selling_price=699.99,
        cost_price=400,
        amount=40
    )

    with SessionLocal() as session:
        session.add_all([p1, p2, p3, p4, p5])
        session.commit()
    #endregion

    # print(session.query(Product).all())
    consumer = DbConsumer(SessionLocal)
    consumer.start_consuming()