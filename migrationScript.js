
//CONFIGURACIONES:

//**IMPORTANTE**//
    //Completar las expresiones con ESTA_NOTACION con los valores que corresoponda.

//CosmosDB
const CosmosClient = require('@azure/cosmos').CosmosClient;
const endpoint = ENDPOINT;
const key = KEY;
const client = new CosmosClient({ endpointCosmosDB: endpoint, keyCosmosDB: key });
const databaseIdCosmosDB = DATABASE_ID_COSMOS_DB;
const containerIdCosmosDB = CONTAINER_ID_COSMOS_DB;

const queryCosmosDB = QUERY_COSMOS_DB; //Query para leer datos de la DB
const deleteCosmosDB = BOOL; //Bool para decidir si borrar la DB de cosmos una vez actualizado el Datalake


//Azure Datalake
const {
    DataLakeServiceClient,
    StorageSharedKeyCredential 
  } = require("@azure/storage-file-datalake");
const accountName = ACCOUNT_NAME;
const accountKey = ACCOUNT_KEY;
const sharedKeyCredential = new StorageSharedKeyCredential(accountName, accountKey);
const datalakeServiceClient = new DataLakeServiceClient( `https://${accountName}.dfs.core.windows.net`, sharedKeyCredential );
const fileSystemName = FILE_SYSTEM_NAME;
const fileSystemClient = datalakeServiceClient.getFileSystemClient(fileSystemName);

const fileNameDatalakeRead = FILE_NAME_DATALAKE_READ; //Nombre del archivo a leer en el Datalake
const fileNameDatalakePersist = FILE_NAME_DATALAKE_READ; //Nombre del archivo a persistir en el Datalake


//SCRIPT
module.exports = async function (context, myTimer) {
    var timeStamp = new Date().toISOString();
    
    if (myTimer.IsPastDue){
        context.log('JavaScript is running late!');
    }
    context.log('JavaScript timer trigger function ran!', timeStamp); 

    //Lectura de datos del datalake
    const datalakeData = await readDatalake();

    //Lectura de datos de CosmosDB
    const cosmosData = await readCosmosDB();

    //Actualizacion del DataLake con los nuevos datos de CosmosDB
    await updateDatalake(datalakeData, cosmosData);  

};

const readCosmosDB = async () => {
     const querySpec = {
         query: queryCosmosDB
     };
     try {
         const { resources } = await client.database(databaseIdCosmosDB).container(containerIdCosmosDB).items.query(querySpec, { enableCrossPartitionQuery: false }).fetchAll();
         return resources;
     } catch (error) {
         console.error(error);
     }
    
 };

 async function readDatalake() {
    let fileClient = fileSystemClient.getFileClient(fileNameDatalakeRead);
    let downloadResponse = await fileClient.read();
    const downloaded = await streamToBuffer(downloadResponse.readableStreamBody);

    return JSON.parse(downloaded.toString());
}

async function updateDatalake(datalakeData, cosmosData){

    //Adaptar logica de actualizacion del Datalake de acuerdo a la necesidad

    const persistSuccess = await persistDatalake(dataLakeUsersToPersist);
    if(persistSuccess && deleteCosmosDB){
        for(let i = 0; i < cosmosUsersToPersist.length; i++){
            await deleteCosmos(cosmosUsersToPersist[i]);
        }
    }else{
        console.log("Error al guardar los datos en el Datalake");
    }
    
}

async function persistDatalake(updatedInfo) {
    
    updatedInfo = JSON.stringify(updatedInfo);
    const streamContents = Buffer.from(updatedInfo);  

    try{
        const fileClient = fileSystemClient.getFileClient(fileNameDatalakePersist);
        await fileClient.create();
        await fileClient.append(streamContents, 0, streamContents.length );
        await fileClient.flush(streamContents.length);

        return true;
    }catch(err){
        console.error(err);
    }

}

async function streamToBuffer(readableStream) {
    return new Promise((resolve, reject) => {
        const chunks = [];
        readableStream.on("data", (data) => {
        chunks.push(data instanceof Buffer ? data : Buffer.from(data));
        });
        readableStream.on("end", () => {
        resolve(Buffer.concat(chunks));
        });
        readableStream.on("error", reject);
    });
}

async function deleteCosmos(persist){
    await client.database(databaseIdCosmosDB).container(containerIdCosmosDB).items.upsert(persist);
}