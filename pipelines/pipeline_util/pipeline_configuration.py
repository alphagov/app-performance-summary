class PipelineConfiguration:
    """
    Maintains settings which are treated as constants by the rest of
    the pipeline.  Most of the settings refer to:
        - which date format should be used for file names and the time
          stamp format used in logging.
        - the names of directories that appear when the pipelines are run
        - standardised parameter values for CSV file processing
    """
    DATE_OUTPUT_FORMAT = '%Y-%m-%d'
    TIME_STAMP_FORMAT = '%b %d, %Y %H.%M.%S'
