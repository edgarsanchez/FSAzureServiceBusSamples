open System
open System.Text
open System.Threading.Tasks
open Microsoft.Azure.ServiceBus

let processMessagesAsync (queueClient: IQueueClient) (message: Message) token =
    // Process the message
    printfn "Received message: SequenceNumber: %d Body: %s" message.SystemProperties.SequenceNumber (Encoding.UTF8.GetString message.Body)

    // Complete the message so that it is not received again.
    // This can be done only if the queueClient is created in ReceiveMode.PeekLock mode (which is default).
    queueClient.CompleteAsync message.SystemProperties.LockToken |> Async.AwaitTask |> Async.StartAsTask :> Task
    // queueClient.CompleteAsync message.SystemProperties.LockToken

    // Note: Use the cancellationToken passed as necessary to determine if the queueClient has already been closed.
    // If queueClient has already been Closed, you may chose to not call CompleteAsync() or AbandonAsync() etc. calls 
    // to avoid unnecessary exceptions.

// Use this Handler to look at the exceptions received on the MessagePump
let exceptionReceivedHandler (exceptionReceivedEventArgs: ExceptionReceivedEventArgs) =
    printfn "Message handler encountered an exception %O." exceptionReceivedEventArgs.Exception
    let context = exceptionReceivedEventArgs.ExceptionReceivedContext
    printfn "Exception context for troubleshooting:"
    printfn "- Endpoint: %s" context.Endpoint
    printfn "- Entity Path: %s" context.EntityPath
    printfn "- Executing Action: %s" context.Action
    Task.CompletedTask

let registerOnMessageHandlerAndReceiveMessages (queueClient: IQueueClient) =
    // Configure the MessageHandler Options in terms of exception handling, number of concurrent messages to deliver etc.
    let messageHandlerOptions = 
        MessageHandlerOptions (Func<_,_> (exceptionReceivedHandler),
            // Maximum number of Concurrent calls to the callback `ProcessMessagesAsync`, set to 1 for simplicity.
            // Set it according to how many messages the application wants to process in parallel.
            MaxConcurrentCalls = 1,
            // Indicates whether MessagePump should automatically complete the messages after returning from User Callback.
            // False below indicates the Complete will be handled by the User Callback as in `ProcessMessagesAsync` below.
            AutoComplete = false )

    // Register the function that will process messages
    queueClient.RegisterMessageHandler (processMessagesAsync queueClient, messageHandlerOptions)

let sendMessagesAsync (queueClient: IQueueClient) numberOfMessagesToSend =
    async {
        try
            for i in 1 .. numberOfMessagesToSend do
                // Create a new message to send to the queue
                let messageBody = sprintf "Message %d" i
                let message = Message (Encoding.UTF8.GetBytes messageBody)

                // Write the body of the message to the console
                printfn "Sending message: %s" messageBody

                // Send the message to the queue
                do! queueClient.SendAsync message |> Async.AwaitTask
        with
        | ex ->
            printfn "%O :: Exception: %s" DateTime.Now ex.Message
    }

let mainAsync serviceBusConnectionString queueName =
    let numberOfMessages = 10
    let queueClient = QueueClient (serviceBusConnectionString, entityPath = queueName)

    printfn "======================================================"
    printfn "Press any key to exit after receiving all the messages."
    printfn "======================================================"

    // Register QueueClient's MessageHandler and receive messages in a loop
    registerOnMessageHandlerAndReceiveMessages queueClient

    async {
        // Send Messages
        do! sendMessagesAsync queueClient numberOfMessages

        Console.ReadKey () |> ignore

        do! queueClient.CloseAsync() |> Async.AwaitTask
    }

let findArgValue argv argName =
    match Array.tryFindIndex ((=) argName) argv with
    | Some i    -> Some argv.[i + 1]
    | None      -> None

[< EntryPoint >]
let main argv =
    match findArgValue argv "-ConnectionString", findArgValue argv "-QueueName" with
    | Some serviceBusConnectionString, Some queueName -> 
        mainAsync serviceBusConnectionString queueName |> Async.RunSynchronously
    | _ ->
        printfn "Specify -ConnectionString and -QueueName to execute the example."

    0
