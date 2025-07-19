from sqlalchemy import Column, Integer, String, Numeric, DateTime, func
from sqlalchemy.orm import relationship

from . import Base

class Product(Base):
    __tablename__ = 'products'
    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String, nullable=False)
    description = Column(String)
    selling_price = Column(Numeric(10,2), nullable=False)
    cost_price = Column(Numeric(10,2), nullable=False)
    amount = Column(Integer, nullable=False)
    created_at = Column(DateTime, default=func.now())

    order_lines = relationship('OrderLine', backref='product')

    def __repr__(self):
        return f'<Product(id={self.id}, name={self.name}), amount={self.amount}>'