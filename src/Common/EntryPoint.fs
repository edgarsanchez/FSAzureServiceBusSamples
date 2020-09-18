module EntryPoint
    open System
    open System.IO

    // This is a common entry point class for all samples. It loads the properties
    // stored in the "azure-msg-samples.properties" file from the user profile
    // and then allows override of the settings from environment variables.

    let basicQueueName = "BasicQueue"
    let partitionedQueueName = "PartitionedQueue"
    let dupdetectQueueName = "DupdetectQueue"
    let basicTopicName = "BasicTopic"
    let sessionQueueName = "SessionQueue"
    let basicQueue2Name = "BasicQueue2"
    let connectionStringName = "SB_SAMPLES_CONNECTIONSTRING"
    let samplePropertiesFileName = "azure-msg-config.properties"

    let rec findConnectionString (fs: StreamReader) =
        if fs.EndOfStream then
            None
        else
            match fs.ReadLine () with
            | null ->
                findConnectionString fs
            | readLine ->
                let lineWithoutComment = Array.head (readLine.Split '#')
                let keyValueParts = lineWithoutComment.Split '='
                if Array.length keyValueParts <= 1 then
                    findConnectionString fs
                elif (Array.head keyValueParts).Trim () = connectionStringName then
                    Some (keyValueParts.[1].Trim ())
                else
                    findConnectionString fs

    let runSample _ run =
        let connectionString =
            match Environment.GetEnvironmentVariable connectionStringName with
            | null ->
                // If there is no environment variable for the connection string, try to get it from a .properties file
                let settingsFile = Path.Combine (Environment.GetFolderPath Environment.SpecialFolder.UserProfile, samplePropertiesFileName)
                if File.Exists settingsFile then
                    use fs = new StreamReader (settingsFile)
                    findConnectionString fs
                else
                    None
            | envVarValue ->
                Some envVarValue

        run connectionString |> Async.RunSynchronously
