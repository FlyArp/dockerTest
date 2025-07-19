
from sqlalchemy import Column, Integer, Numeric, DateTime, func, String
from sqlalchemy.orm import relationship

from . import Base


class Order(Base):
    __tablename__ = 'orders'

    id = Column(Integer, primary_key=True, autoincrement=True)
    customer_name = Column(String)
    total_amount = Column(Numeric(10,2), nullable=False)
    order_status = Column(String, default='Pending')
    created_at = Column(DateTime, default=func.now())

    product_lines = relationship('OrderLine', backref='order')

    def __repr__(self):
        return f'<Order(id={self.id}), customer_name={self.customer_name}, total_amount={self.total_amount}>'


