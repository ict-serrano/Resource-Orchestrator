from distutils.core import setup

setup(
    name='SERRANO - Resource Orchestrator',
    version='2.0',
    description='SERRANO - Resource Orchestrator',
    author='Aristotelis Kretsis',
    author_email='akretsis@mail.ntua.gr',
    url='',
    packages=[
        'serrano_orchestrator',
        'serrano_orchestrator.drivers',
        'serrano_orchestrator.orchestration_api',
        'serrano_orchestrator.orchestration_manager',
        'serrano_orchestrator.utils',
        'serrano_rot',
        'serrano_rot.api'
    ]
)
