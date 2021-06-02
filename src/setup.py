import subprocess

import setuptools

import setuptools.command.install as _install

# import distutils.command.build as _build
from distutils.command.build import build as _build

CUSTOM_COMMANDS = [
    ['apt-get', 'update'],
    ['apt-get', '--assume-yes', 'install', 'libxml2-dev', 'wget', 'unzip'],
    ['pip',
     'install',
     'https://github.com/explosion/spacy-models/releases/download/en_core_web_lg-2.3.0/en_core_web_lg-2.3.0.tar.gz'
     ],
]


# This class handles the pip install mechanism.
class Build(_build):  # pylint: disable=invalid-name
    """A build command class that will be invoked during package install.
    The package built using the current setup.py will be staged and later
    installed in the worker using `pip install package'. This class will be
    instantiated during install for this specific scenario and will trigger
    running the custom commands specified.
    """

    sub_commands = _build.sub_commands + [('CustomCommands', None)]


class CustomCommands(setuptools.Command):
    """A setuptools Command class able to run arbitrary commands."""

    def initialize_options(self):
        pass

    def finalize_options(self):
        pass

    def RunCustomCommand(self, command_list):
        print('Running command: %s' % command_list)
        p = subprocess.Popen(
            command_list,
            stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        # Can use communicate(input='y\n'.encode()) if the command run requires
        # some confirmation.
        stdout_data, stdout_err = p.communicate()
        print('Command output: %s | Command err: %s' % (stdout_data, stdout_err))
        if p.returncode != 0:
            raise RuntimeError(
                'Command %s failed: exit code: %s' % (command_list, p.returncode)
            )

    def run(self):
        for command in CUSTOM_COMMANDS:
            self.RunCustomCommand(command)


REQUIRED_PACKAGES = [
    'apache-beam[gcp]==2.28.0',
    'more-itertools==5.0.0',
    'spacy==2.3.0',
    'tldextract==3.1.0'
]

setuptools.setup(
    name='nlp_preprocessing',
    version='0.0.1',
    install_requires=REQUIRED_PACKAGES,
    packages=setuptools.find_packages(),
    cmdclass={
        # Command class instantiated and run during pip install scenarios.
        'build': Build,
        'CustomCommands': CustomCommands,
    },
    include_package_data=True,
    description="Ingest Data From Hackernews Table"
)
