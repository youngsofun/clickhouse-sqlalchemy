
tests_require = [
    'pytest',
    'sqlalchemy>=1.4,<1.5',
    'requests',
    'responses',
    'parameterized'
]

try:
    from pip import main as pipmain
except ImportError:
    from pip._internal import main as pipmain

pipmain(['install'] + tests_require)
