This function answering to requests from Yandex Alice Smart Home API.

More description is coming soon...

[yandex smart home api](https://yandex.ru/dev/dialogs/smart-home/doc/en/reference/post-devices-query-jrpc) is used to communicate between smart home elements

[yandex dialogs platform](https://dialogs.yandex.ru/developer) allow us to connect yandex function with yandex smart home

I use [platformio](https://platformio.org/platformio-ide) with vscode to develop firmware on C++ for my devices.

You need to be registered on [yandex cloud](https://console.yandex.cloud/), payment method added.

[smart home with Alice app](https://play.google.com/store/apps/details?id=com.yandex.iot&hl=en)

The reaction on action in app looks as follows

- we push the button in the app
- it sends the request to yandex cloud
- serverless funciton is loading
- it sends request to device and subscribe to its topic
- device get request and send response to topic
- function receives the rsponse and send it to yandex smart home app
