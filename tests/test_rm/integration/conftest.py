def pytest_addoption(parser):
    parser.addoption("--resource", action="store", default="xsede.wrangler_ssh")

def pytest_generate_tests(metafunc):
    option_value = metafunc.config.option.resource
    if 'resource' in metafunc.fixturenames and option_value is not None:
        metafunc.parametrize("resource", [option_value])
