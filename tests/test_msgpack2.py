import datetime
import importlib
import io
import collections
import edgar_code.msgpack2 as msgpack2
from edgar_code.parse import Index


def test_msgpack2() -> None:
    test_cases = [
        1,
        datetime.date.today(),
        dict(a=3, b=5),
        {3, 4},
        'hello',
        b'hello',
        ValueError(),
        collections.Counter(d=3, e=4),
        Index(
            company_name='Google',
            form_type='10-K',
            CIK=120000000,
            date_filed=datetime.date(1995, 1, 1), year=1995, qtr=1,
            url='https://url/',
        ),
        # composition
        (datetime.date.today(), dict(a=3, b=5), [3, Index(
            company_name='Google',
            form_type='10-K',
            CIK=120000000,
            date_filed=datetime.date(1995, 1, 1), year=1995, qtr=1,
            url='https://url/',
        )]),
    ]


    for obj in test_cases:
        assert msgpack2.loads(msgpack2.dumps(obj)) == obj or isinstance(obj, Exception)

    for obj in test_cases[:2]:
        fil = io.BytesIO()
        msgpack2.dump(obj, fil)
        fil.seek(0)
        assert msgpack2.load(fil) == obj or isinstance(obj, Exception)
