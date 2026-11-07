from decimal import Decimal


def calculate(kind, amount, points, balance, purchase=None, already_returned=False):
    """Бизнес-правила: без брокера и базы данных."""
    if kind == "purchase":
        if purchase is not None:
            return 0, "rejected", "duplicate_purchase"
        return int(amount * Decimal("0.05")), "applied", None

    if kind == "return":
        if purchase is None:
            return 0, "rejected", "purchase_not_found"
        if purchase["customer_id"] != balance["customer_id"]:
            return 0, "rejected", "customer_mismatch"
        if already_returned:
            return 0, "rejected", "already_returned"
        return -int(purchase["points_delta"]), "applied", None

    if points > amount * Decimal("0.30"):
        return 0, "rejected", "limit_exceeded"
    if points > balance["balance"]:
        return 0, "rejected", "insufficient_balance"
    return -points, "applied", None
