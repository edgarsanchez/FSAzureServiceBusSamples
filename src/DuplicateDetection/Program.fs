open System
open Microsoft.Azure.ServiceBus
open Microsoft.Azure.ServiceBus.Core

let send (connectionString: string) =
    // Create a sender over the previously configured duplicate-detection
    // enabled queue.
    let sender = MessageSender (connectionString, EntryPoint.dupdetectQueueName)

    // Create the message-id 
    let messageId = string (Guid.NewGuid ())
    
    printfn "\tSending messages to %s ..." sender.Path
    async {
        // send the first message using the message-id
        let message = Message (MessageId = messageId, TimeToLive = TimeSpan.FromMinutes 1.)
        do! sender.SendAsync message |> Async.AwaitTask
        printfn "\t=> Sent a message with messageId %s" message.MessageId

        // send the second message using the message-id
        let message2 = Message (MessageId = messageId, TimeToLive = TimeSpan.FromMinutes 1.)
        do! sender.SendAsync message2 |> Async.AwaitTask
        printfn "\t=> Sent a duplicate message with messageId %s" message.MessageId
        
        do! sender.CloseAsync () |> Async.AwaitTask
    }

let receive (connectionString: string) =
    // create receiver over the duplicate-detection enabled queue
    let receiver = MessageReceiver (connectionString, EntryPoint.dupdetectQueueName, ReceiveMode.PeekLock)

    let rec receiveMessages (previousMessageId: string) =
        async {
            // receive a message
            let! receivedMessage = receiver.ReceiveAsync (TimeSpan.FromSeconds 5.) |> Async.AwaitTask
            if not (isNull receivedMessage) then
                // complete the received message
                printfn "\t<= Received a message with messageId %s" receivedMessage.MessageId
                do! receiver.CompleteAsync receivedMessage.SystemProperties.LockToken |> Async.AwaitTask

                if previousMessageId.Equals(receivedMessage.MessageId, StringComparison.OrdinalIgnoreCase) then
                    raise (Exception ("Received a duplicate messsage"))
                
                do! receiveMessages receivedMessage.MessageId
        }

    printfn "\n\tWaiting up to 5 seconds for messages from %s ..." receiver.Path
    async {
        do! receiveMessages ""
        do! receiver.CloseAsync () |> Async.AwaitTask

        printfn "\tDone receiving messages from %s" receiver.Path
    }

let run connectionString =
    async {
        match connectionString with
        | None ->
            printfn "No ServiceBus connection string provided"
        | Some connectionString ->
            // first send the two messages
            do! send connectionString
            // then retrieve the messages
            do! receive connectionString
    }
         
[< EntryPoint >]
let main argv =
    try
        EntryPoint.runSample argv run
        0
    with
    | exn ->
        printfn "%O" exn
        1
