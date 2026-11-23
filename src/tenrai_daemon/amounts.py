"""Convert between exact TRAI text and integer atoms."""

from decimal import Context, Decimal, InvalidOperation

ATOMS_PER_TRAI = 100_000_000
DECIMALS = 8


def to_atoms(amount: str | Decimal) -> int:
    """Convert a TRAI string or Decimal to atoms."""
    if isinstance(amount, float):
        raise TypeError("pass TRAI amounts as str or Decimal, never as float")
    try:
        value = Decimal(str(amount).strip())
    except InvalidOperation:
        raise ValueError(f"not a TRAI amount: {amount!r}") from None
    if not value.is_finite() or value < 0:
        raise ValueError(f"not a TRAI amount: {amount!r}")
    if value.is_zero():
        return 0
    _, digits, exponent = value.as_tuple()
    shift = exponent + DECIMALS
    if shift < 0 and (len(digits) + shift <= 0 or any(digits[shift:])):
        raise ValueError(f"TRAI has {DECIMALS} decimal places at most: {amount!r}")
    return int(value.scaleb(DECIMALS, context=Context(prec=len(digits))))


def from_atoms(atoms: int) -> str:
    """Format atoms as TRAI with eight decimal places."""
    if isinstance(atoms, bool) or not isinstance(atoms, int) or atoms < 0:
        raise ValueError(f"atoms must be a non-negative int: {atoms!r}")
    whole, fraction = divmod(atoms, ATOMS_PER_TRAI)
    return f"{whole}.{fraction:08d}"


def amount_text(amount: str | Decimal) -> str:
    """Validate and format a daemon amount."""
    atoms = to_atoms(amount)
    if atoms == 0:
        raise ValueError("an amount must be above zero")
    return from_atoms(atoms)
