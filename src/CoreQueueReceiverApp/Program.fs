open System
open System.Text
open System.Threading.Tasks
open Microsoft.Azure.ServiceBus

// Connection String for the namespace can be obtained from the Azure portal under the 
// 'Shared Access policies' section.
let serviceBusConnectionString = "<your_connection_string>"
let queueName = "<your_queue_name>"

let exceptionReceivedHandler (exceptionReceivedEventArgs: ExceptionReceivedEventArgs) =
    printfn "Message handler encountered an exception %O" exceptionReceivedEventArgs.Exception
    let context = exceptionReceivedEventArgs.ExceptionReceivedContext
    printfn "Exception context for troubleshooting:"
    printfn "- Endpoint: %s" context.Endpoint
    printfn "- Entity Path: %s" context.EntityPath
    printfn "- Executing Action: %s" context.Action
    Task.CompletedTask

let processMessagesAsync (queueClient: IQueueClient) (message: Message) token =
    // Process the message
    printfn "Received message: SequenceNumber:%d Body:%s" message.SystemProperties.SequenceNumber (Encoding.UTF8.GetString message.Body)

    // Complete the message so that it is not received again.
    // This can be done only if the queueClient is created in ReceiveMode.PeekLock mode (which is default).
    queueClient.CompleteAsync message.SystemProperties.LockToken |> Async.AwaitTask |> Async.StartAsTask :> Task
    
    // Note: Use the cancellationToken passed as necessary to determine if the queueClient has already been closed.
    // If queueClient has already been Closed, you may chose to not call CompleteAsync() or AbandonAsync() etc. calls 
    // to avoid unnecessary exceptions.

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
    queueClient.RegisterMessageHandler (Func<_,_,_> (processMessagesAsync queueClient), messageHandlerOptions)
     
[< EntryPoint >]
let main _ =
    let queueClient = QueueClient (serviceBusConnectionString, queueName)

    printfn "======================================================"
    printfn "Press ENTER key to exit after receiving all the messages."
    printfn "======================================================"

    // Register QueueClient's MessageHandler and receive messages in a loop
    registerOnMessageHandlerAndReceiveMessages queueClient

    Console.ReadKey () |> ignore

    queueClient.CloseAsync().Wait ()

    0
