from solutions.IWC.constants import DEFAULT_EXECUTION_ORDER, BANK_STATEMENTS_EXECUTION_ORDER


@dataclass
class Provider:
    name: str
    base_url: str
    depends_on: list[str]
    """Modifier to the order in which tasks will be executed."""
    execution_order: int



COMPANIES_HOUSE_PROVIDER = Provider(
    name="companies_house", base_url="https://fake.companieshouse.co.uk", depends_on=[], execution_order=DEFAULT_EXECUTION_ORDER
)


CREDIT_CHECK_PROVIDER = Provider(
    name="credit_check",
    base_url="https://fake.creditcheck.co.uk",
    depends_on=["companies_house"],
    execution_order=DEFAULT_EXECUTION_ORDER
)


BANK_STATEMENTS_PROVIDER = Provider(
    name="bank_statements", base_url="https://fake.bankstatements.co.uk", depends_on=[], execution_order=BANK_STATEMENTS_EXECUTION_ORDER
)

ID_VERIFICATION_PROVIDER = Provider(
    name="id_verification", base_url="https://fake.idv.co.uk", depends_on=[], execution_order=DEFAULT_EXECUTION_ORDER
)

REGISTERED_PROVIDERS: list[Provider] = [
    BANK_STATEMENTS_PROVIDER,
    COMPANIES_HOUSE_PROVIDER,
    CREDIT_CHECK_PROVIDER,
    ID_VERIFICATION_PROVIDER,
]
