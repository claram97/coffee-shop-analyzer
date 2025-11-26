"""
Defines a set of data classes that represent the raw, unprocessed structure of
records received from the client.

These classes act as Data Transfer Objects (DTOs), serving as the initial,
in-memory representation of data before any filtering, validation, or
transformation is applied.
"""


class RawMenuItems:
    """Represents a single, unprocessed menu item record."""

    def __init__(
        self,
        item_id: str,
        name: str,
        price: str,
        category: str,
        is_seasonal: str,
        available_from: str,
        available_to: str,
    ):
        """
        Initializes a RawMenuItems instance.

        Args:
            item_id: The unique identifier for the product.
            name: The name of the menu item.
            price: The price of the item.
            category: The category the item belongs to.
            is_seasonal: A flag indicating if the item is seasonal.
            available_from: The date from which the item is available.
            available_to: The date until which the item is available.
        """
        self.item_id = item_id
        self.name = name
        self.price = price
        self.category = category
        self.is_seasonal = is_seasonal
        self.available_from = available_from
        self.available_to = available_to


class RawStore:
    """Represents a single, unprocessed store record."""

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
        """
        Initializes a RawStore instance.

        Args:
            store_id: The unique identifier for the store.
            store_name: The name of the store.
            street: The street address of the store.
            postal_code: The postal code of the store's location.
            city: The city where the store is located.
            state: The state or province where the store is located.
            latitude: The geographical latitude of the store.
            longitude: The geographical longitude of the store.
        """
        self.store_id = store_id
        self.store_name = store_name
        self.street = street
        self.postal_code = postal_code
        self.city = city
        self.state = state
        self.latitude = latitude
        self.longitude = longitude


class RawTransactionItem:
    """Represents a single item line within a transaction."""

    def __init__(
        self,
        transaction_id: str,
        item_id: str,
        quantity: str,
        unit_price: str,
        subtotal: str,
        created_at: str,
    ):
        """
        Initializes a RawTransactionItem instance.

        Args:
            transaction_id: The identifier of the parent transaction.
            item_id: The unique identifier for the product/item sold.
            quantity: The number of units of the item sold.
            unit_price: The price of a single unit of the item.
            subtotal: The total price for this line item (quantity * unit_price).
            created_at: The timestamp when the transaction item was recorded.
        """
        self.transaction_id = transaction_id
        self.item_id = item_id
        self.quantity = quantity
        self.unit_price = unit_price
        self.subtotal = subtotal
        self.created_at = created_at


class RawTransaction:
    """Represents a single, unprocessed transaction header record."""

    def __init__(
        self,
        transaction_id: str,
        store_id: str,
        payment_method_id: str,
        voucher_id: str,
        user_id: str,
        original_amount: str,
        discount_applied: str,
        final_amount: str,
        created_at: str,
    ):
        """
        Initializes a RawTransaction instance.

        Args:
            transaction_id: The unique identifier for the transaction.
            store_id: The identifier of the store where the transaction occurred.
            payment_method_id: The identifier for the payment method used.
            voucher_id: The identifier for any voucher applied to the transaction.
            user_id: The identifier of the user who made the purchase.
            original_amount: The total amount before any discounts.
            discount_applied: The amount of discount applied to the transaction.
            final_amount: The final amount paid after discounts.
            created_at: The timestamp when the transaction was created.
        """
        self.transaction_id = transaction_id
        self.store_id = store_id
        self.payment_method_id = payment_method_id
        self.voucher_id = voucher_id
        self.user_id = user_id
        self.original_amount = original_amount
        self.discount_applied = discount_applied
        self.final_amount = final_amount
        self.created_at = created_at


class RawUser:
    """Represents a single, unprocessed user record."""

    def __init__(self, user_id: str, gender: str, birthdate: str, registered_at: str):
        """
        Initializes a RawUser instance.

        Args:
            user_id: The unique identifier for the user.
            gender: The gender of the user.
            birthdate: The user's date of birth.
            registered_at: The timestamp when the user registered.
        """
        self.user_id = user_id
        self.gender = gender
        self.birthdate = birthdate
        self.registered_at = registered_at


class RawTransactionStore:
    """Represents a transaction record joined with store information."""

    def __init__(
        self,
        transaction_id: str,
        store_id: str,
        store_name: str,
        city: str,
        final_amount: str,
        created_at: str,
        user_id: str,
        **kwargs,
    ):
        self.transaction_id = transaction_id
        self.store_id = store_id
        self.store_name = store_name
        self.city = city
        self.final_amount = final_amount
        self.created_at = created_at
        self.user_id = user_id


class RawTransactionItemMenuItem:
    """Represents a transaction item joined with its corresponding menu item."""

    def __init__(
        self,
        transaction_id: str,
        item_name: str,
        quantity: str,
        subtotal: str,
        created_at: str,
        **kwargs,
    ):
        self.transaction_id = transaction_id
        self.item_name = item_name
        # Ensure quantity is never empty/None
        self.quantity = "0" if quantity is None or quantity == "" else quantity
        self.subtotal = subtotal
        self.created_at = created_at

    def __str__(self):
        """String representation for debugging"""
        return f"RawTransactionItemMenuItem(id={self.transaction_id}, name={self.item_name}, quantity={self.quantity}, subtotal={self.subtotal})"


class RawTransactionStoreUser:
    """Represents a transaction joined with store and user information."""

    def __init__(
        self,
        transaction_id: str,
        store_id: str,
        store_name: str,
        user_id: str,
        birthdate: str,
        created_at: str,
        **kwargs,
    ):
        self.transaction_id = transaction_id
        self.store_id = store_id
        self.store_name = store_name
        self.user_id = user_id
        self.birthdate = birthdate
        self.created_at = created_at


# --- Query Result Entities ---


class ResultFilteredTransaction:
    """Represents a filtered transaction for Query 1 result."""

    def __init__(self, transaction_id: str, final_amount: str):
        self.transaction_id = transaction_id
        self.final_amount = final_amount


class ResultProductMetrics:
    """Represents a product metric entry for Query 2 result."""

    def __init__(
        self, month: str, name: str, quantity: str = None, revenue: str = None
    ):
        self.month = month
        self.name = name
        if quantity is not None:
            self.quantity = str(quantity)
        if revenue is not None:
            self.revenue = str(revenue)

    def __str__(self):
        """String representation for debugging"""
        quantity = getattr(self, "quantity", None)
        revenue = getattr(self, "revenue", None)
        return f"ResultProductMetrics(month={self.month}, name={self.name}, quantity={quantity}, revenue={revenue})"


class ResultStoreTPV:
    """Represents a store TPV metric for Query 3 result."""

    def __init__(self, store_name: str, period: str, amount: str):
        self.store_name = store_name
        self.period = period
        self.amount = amount


class ResultTopCustomer:
    """Represents a top customer entry for Query 4 result."""

    def __init__(self, store_name: str, birthdate: str, purchase_count: str):
        self.store_name = store_name
        self.birthdate = birthdate
        self.purchase_count = purchase_count


class ResultError:
    """Represents an error result for any query."""

    def __init__(
        self,
        query_id: str,
        error_code: str = "EXECUTION_ERROR",
        error_message: str = "",
    ):
        self.query_id = query_id
        self.error_code = error_code
        self.error_message = error_message
