let socket = new WebSocket("ws://127.0.0.1:5050");

socket.onopen = () => {
    console.log("found server");

    socket.send("js says hi");
}

socket.onmessage = (msg) => {
    console.log("msg", msg);
}

function send() {
    socket.send("manualsend");
}
