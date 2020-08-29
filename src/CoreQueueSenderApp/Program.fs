open System
open System.Text
open Microsoft.Azure.ServiceBus

// Connection String for the namespace can be obtained from the Azure portal under the 
// 'Shared Access policies' section.
let serviceBusConnectionString = "<your_connection_string>"
let queueName = "<your_queue_name>"

let sendMessagesAsync (queueClient: QueueClient) numberOfMessagesToSend =
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
        with ex ->
            printfn "%O :: Exception: %s" DateTime.Now ex.Message
    }

[< EntryPoint >]
let main _ =
    let numberOfMessages = 10
    let queueClient = QueueClient (serviceBusConnectionString, queueName)

    printfn "======================================================"
    printfn "Press ENTER key to exit after sending all the messages."
    printfn "======================================================"

    async {
        // Send messages.
        do! sendMessagesAsync queueClient numberOfMessages

        Console.ReadKey () |> ignore

        do! queueClient.CloseAsync () |> Async.AwaitTask    
    } |> Async.RunSynchronously

    0
