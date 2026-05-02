from unittest.mock import Mock

FAKE_INVENTORY_DICT = {
    "Laptop": {"price": 1000.0, "amount": 5},
    "Mouse": {"price": 100.0, "amount": 10},
}

def _create_mock_product(name, price, amount):
    product = Mock()
    product.name = name
    product.selling_price = price
    product.amount = amount
    return product


def get_default_products():
    return [
        _create_mock_product('laptop', 1000.0, 5),
        _create_mock_product('mouse', 100.0, 10)
    ]