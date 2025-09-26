"""
Raw data entities for the coffee shop analyzer protocol.
These classes represent the unprocessed data structures as received from clients.
"""


class RawMenuItems:
    """Raw menu item data as received from client."""
    
    def __init__(
        self,
        product_id: str,
        name: str,
        price: str,
        category: str,
        is_seasonal: str,
        available_from: str,
        available_to: str,
    ):
        self.product_id = product_id
        self.name = name
        self.price = price
        self.category = category
        self.is_seasonal = is_seasonal
        self.available_from = available_from
        self.available_to = available_to


class RawStore:
    """Raw store data as received from client."""
    
    def __init__(
        self,
        store_id: str,
        store_name: str,
        street: str,
        postal_code: str,
        city: str,
        state: str,
        latitude: str,
        longitude: str,
    ):
        self.store_id = store_id
        self.store_name = store_name
        self.street = street
        self.postal_code = postal_code
        self.city = city
        self.state = state
        self.latitude = latitude
        self.longitude = longitude


class RawTransactionItem:
    """Raw transaction item data as received from client."""
    
    def __init__(
        self,
        transaction_id: str,
        item_id: str,
        quantity: str,
        unit_price: str,
        subtotal: str,
        created_at: str,
    ):
        self.transaction_id = transaction_id
        self.item_id = item_id
        self.quantity = quantity
        self.unit_price = unit_price
        self.subtotal = subtotal
        self.created_at = created_at


class RawTransaction:
    """Raw transaction data as received from client."""
    
    def __init__(
        self,
        transaction_id: str,
        store_id: str,
        payment_method_id: str,
        user_id: str,
        original_amount: str,
        discount_applied: str,
        final_amount: str,
        created_at: str,
    ):
        self.transaction_id = transaction_id
        self.store_id = store_id
        self.payment_method_id = payment_method_id
        self.user_id = user_id
        self.original_amount = original_amount
        self.discount_applied = discount_applied
        self.final_amount = final_amount
        self.created_at = created_at


class RawUser:
    """Raw user data as received from client."""
    
    def __init__(self, user_id: str, gender: str, birthdate: str, registered_at: str):
        self.user_id = user_id
        self.gender = gender
        self.birthdate = birthdate
        self.registered_at = registered_at