let socket = new WebSocket("ws://127.0.0.1:5050");

socket.onopen = (error) => {
    console.log("error", error);
    console.log("found server");

    socket.send("js says hi");
    
    socket.send("another");
}

socket.onmessage = (msg) => {
    console.log("msg", msg);
}

function send() {
    socket.send("manualsend");
}
