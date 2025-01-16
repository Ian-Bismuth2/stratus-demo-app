from sqlalchemy.orm import sessionmaker
import tempfile

from wx_explore.cloud.proxy import HttpResponse, proxy
from wx_explore.cloud.helpers import s3_client, db_engine
from wx_explore.common.models import Source, SourceField
from wx_explore.ingest.reduce_grib import reduce_grib


def func(req):
    """Process GRIB file reduction request with proper resource management.
    
    Args:
        req: Request object containing url, idx, source_name and out parameters
        
    Returns:
        HttpResponse with status and filename
        
    Raises:
        ValueError: If required parameters are missing or source is invalid
        ClientError: If S3 upload fails
    """
    import logging
    from botocore.exceptions import ClientError
    from contextlib import contextmanager

    logger = logging.getLogger(__name__)

    if not all(param in req.args for param in ['url', 'idx', 'source_name', 'out']):
        raise ValueError("Missing params")

    s3 = s3_client(req.args)
    engine = db_engine(req.args)
    Session = sessionmaker()
    Session.configure(bind=engine)

    @contextmanager
    def session_scope():
        """Provide a transactional scope around a series of operations."""
        session = Session()
        try:
            yield session
            session.commit()
        except:
            session.rollback()
            raise
        finally:
            session.close()

    try:
        with session_scope() as session:
            fields = (session.query(SourceField)
                     .join(Source, SourceField.source_id == Source.id)
                     .filter(Source.name == req.args['source_name'])
                     .all())

            if not fields:
                raise ValueError("Provided source_name does not have a Source table entry")

            with tempfile.TemporaryFile() as f:
                try:
                    reduce_grib(req.args['url'], req.args['idx'], fields, f)
                    f.seek(0)
                    
                    try:
                        s3.upload_fileobj(
                            f, 
                            "vtxwx-data", 
                            req.args['out'], 
                            ExtraArgs={'ACL': 'public-read'}
                        )
                    except ClientError as e:
                        logger.error(f"Failed to upload to S3: {str(e)}")
                        raise
                        
                except Exception as e:
                    logger.error(f"Failed to reduce GRIB file: {str(e)}")
                    raise

        return HttpResponse(
            {"status": "ok", "filename": req.args['out']}
        )
        
    except Exception as e:
        logger.error(f"Error processing request: {str(e)}")
        return HttpResponse(
            {"status": "error", "message": str(e)},
            status_code=500
        )

main = proxy(func)