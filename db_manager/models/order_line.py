from sqlalchemy import Column, Integer, ForeignKey
from sqlalchemy.orm import relationship

from . import Base

class OrderLine(Base):
    __tablename__ = 'order_lines'

    id = Column(Integer, primary_key=True, autoincrement=True)
    order_id = Column(Integer, ForeignKey('orders.id'), nullable=False)
    product_id = Column(Integer, ForeignKey('products.id'), nullable=False)
    amount = Column(Integer, nullable=False)

    def __repr__(self):
        return f'<ProductLine(id={self.id}, order_id={self.order_id}, product_id={self.product_id}, amount={self.amount})>'

