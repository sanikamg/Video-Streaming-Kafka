// const videoElement = document.getElementById('videoElement');

// const eventSource = new EventSource('/kafka-stream');

// eventSource.addEventListener('message', (event) => {
//     const frameData = event.data;
//     const blob = new Blob([frameData], { type: 'video/mp4' });
//     const videoUrl = URL.createObjectURL(blob);
//     videoElement.src = videoUrl;
// });

const videoElement = document.getElementById('videoElement');



const eventSource = new EventSource('/kafka-stream');

eventSource.addEventListener('error', (event) => {
    console.error('Error occurred:', event);
});

eventSource.addEventListener('message', (event) => {
    console.log('Received video frame:', event.data);

    const frameData = event.data;
    const blob = new Blob([frameData], { type: 'video/mp4' });
    const videoUrl = URL.createObjectURL(blob);
    console.log(videoUrl);
    videoElement.src = videoUrl;
});



