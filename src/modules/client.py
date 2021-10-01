import oci

def get_client(file_location, account):
    '''
    Configuracion del cliente para la cuenta OCI
    Parameters
    ----------
    file_location : str
        path donde se encuentran el archivo de credenciales OCI
    account: str
        cuenta que se utilizara del archivo de credenciales OCI
    
    Returns
    ---------
    dict
    '''
    # ### configuracion de credenciales ###
    config = oci.config.from_file(file_location=file_location, profile_name=account)
    reporting_bucket = config['tenancy']
    client = oci.object_storage.ObjectStorageClient(config)

    dict_var = {'client': client, 'bucket': reporting_bucket}
    
    return dict_var