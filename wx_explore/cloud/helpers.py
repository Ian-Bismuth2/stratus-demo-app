from sqlalchemy import create_engine
import os


def db_engine(args):
    """Create a database engine with AWS IAM authentication.
    
    Args:
        args: Dictionary containing connection parameters including host and port
        
    Returns:
        SQLAlchemy engine instance
        
    Raises:
        OperationalError: If database connection fails after retries
    """
    import boto3
    import logging
    from botocore.exceptions import ClientError
    from sqlalchemy.exc import OperationalError
    import time

    logger = logging.getLogger(__name__)
    
    if 'CONFIG' in os.environ:
        from wx_explore.common.config import Config
        return create_engine(Config.SQLALCHEMY_DATABASE_URI)

    max_retries = 3
    retry_count = 0
    base_delay = 1  # Base delay in seconds

    while retry_count < max_retries:
        try:
            rds = boto3.client('rds', region_name=os.environ.get('AWS_REGION', 'us-east-1'))
            token = rds.generate_db_auth_token(
                DBHostname=args['host'],
                Port=args['port'],
                Region=os.environ.get('AWS_REGION', 'us-east-1')
            )
            
            connection_url = f"postgresql://{args['username']}:{token}@{args['host']}:{args['port']}/{args['database']}"
            return create_engine(connection_url)

        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code == 'ThrottlingException':
                if retry_count < max_retries - 1:
                    delay = (base_delay * (2 ** retry_count))  # Exponential backoff
                    logger.warning(f"AWS throttling encountered, retrying in {delay} seconds...")
                    time.sleep(delay)
                    retry_count += 1
                    continue
            
            logger.error(f"Failed to authenticate with AWS IAM: {str(e)}")
            raise OperationalError("Failed to authenticate with AWS IAM", e)
            
        except Exception as e:
            logger.error(f"Unexpected error during database connection: {str(e)}")
            raise OperationalError("Database connection failed", e)

    raise OperationalError("Max retries exceeded while attempting to connect to database")