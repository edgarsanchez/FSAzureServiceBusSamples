open System
open System.Text
open Microsoft.Azure.ServiceBus
open Microsoft.Azure.ServiceBus.Core

// Create a new Service Bus message.
let createMessage label =
    let msg = Message (
                Encoding.UTF8.GetBytes (sprintf "This is the body of message \"%s\"." label),
                Label = label,
                TimeToLive = TimeSpan.FromSeconds 90. )
    msg.UserProperties.Add ("Priority", 1)
    msg.UserProperties.Add ("Importance", "High")
    msg

let printReceivedMessage (receivedMessage: Message) =
    async {
        Console.ForegroundColor <- ConsoleColor.Yellow
        do! Console.Out.WriteLineAsync (sprintf "Received message:\n\tLabel:\t%s\n\tBody:\t%s\n" receivedMessage.Label (Encoding.UTF8.GetString receivedMessage.Body)) |> Async.AwaitTask
        for p in receivedMessage.UserProperties do
            do! Console.Out.WriteLineAsync (sprintf "\tProperty:\t%s = %O" p.Key p.Value) |> Async.AwaitTask
        Console.ResetColor ()
    }

// This sample demonstrates how to automatically forward messages from a queue,
// subscription, or deadletter queue into another queue or topic. 
// The sample assumes prior setup of a topology of Service Bus entities 
// as described in the accompanying README file.
let run (connectionStringMaybe: string option) =
    if connectionStringMaybe.IsNone then
        raise (Exception "Connection string missing")

    let connectionString = connectionStringMaybe.Value
    printfn "\nSending messages\n"

    async {
        // Create sender and send message M1 into the source topic
        let topicSender = MessageSender (connectionString, "AutoForwardSourceTopic")
        do! topicSender.SendAsync (createMessage "M1") |> Async.AwaitTask

        // Create sender and send message M2 directly into the target queue
        let queueSender = MessageSender (connectionString, "AutoForwardTargetQueue")
        do! queueSender.SendAsync (createMessage "M2") |> Async.AwaitTask

        // Create the receiver on the target queue
        printfn "\nReceiving messages\n"
        let targetQueueReceiver = MessageReceiver (connectionString, "AutoForwardTargetQueue")
        for i in 0 .. 1 do
            // We are expecting two messages to arrive into the target queue. 
            // 1) Message M2 has been sent directly
            // 2) Message M1 has been auto-forwarded from a subscription on the source topic
            let! message = targetQueueReceiver.ReceiveAsync (TimeSpan.FromSeconds 10.) |> Async.AwaitTask
            if isNull message then
                raise (Exception "Expected message not received.")

            do! printReceivedMessage message
            do! targetQueueReceiver.CompleteAsync message.SystemProperties.LockToken |> Async.AwaitTask
        
        do! targetQueueReceiver.CloseAsync () |> Async.AwaitTask
    }

[<EntryPoint>]
let main argv =
    try
        EntryPoint.runSample argv run
        0
    with
    | exn ->
        printfn "%O" exn
        1
