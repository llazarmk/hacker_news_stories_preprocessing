from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions


class DataflowOptions:
    RUNNER = {'local': 'DirectRunner', 'gcp': 'DataflowRunner'}

    @staticmethod
    def get_options(environment: str,
                    job_name=str,
                    staging_location=str,
                    temp_location=str,
                    project: str = None,
                    region: str = 'europe-west1',
                    machine_type: str = 'n1-standard-4',
                    num_workers: int = 4,
                    save_main_session: bool = True,
                    setup_file: str = './setup.py'):
        options = PipelineOptions(
            runner=DataflowOptions.RUNNER[environment],
            project=project,
            job_name=job_name,
            staging_location=staging_location,
            temp_location=temp_location,
            num_workers=num_workers,
            region=region,
            machine_type=machine_type,
            save_main_session=save_main_session,
            setup_file=setup_file,
            disk_size_gb=30
        ).view_as(GoogleCloudOptions)
        return options


class CustomOptions(PipelineOptions):
    """
    Beam running pipeline
    """

    @classmethod
    def _add_argparse_args(cls, parser):

        parser.add_argument('--bq_input_table',
                            help='big query hacker news stories table')

        parser.add_argument('--bq_output_table',help='big query output table')
