const BUCKET = 'demoanalyticsapp-s3'

var app = new Vue({
    el: '#app',
    data() {
        return {
            s3: {},
            video: {},
            canvas: {},
            captures: []
        }
    },
    mounted () {
        var self = this
        //this.video = this.$refs.video
        if(navigator.mediaDevices && navigator.mediaDevices.getUserMedia) {
            navigator.mediaDevices.getUserMedia({ video: true })
            .then(stream => {
                self.video = self.$refs.video
                if ("srcObject" in video) {
                    self.video.srcObject = stream
                  } else {
                    // Avoid using this in new browsers, as it is going away.
                    self.video.src = window.URL.createObjectURL(stream);
                  }
                  self.video.onloadedmetadata = (e) => { 
                    self.video.play()
                        .then(() => { console.log('Playing started') })
                        .catch((e) => { console.log('Autoplay is disabled') })
                  }
            })
            .catch(err => { console.log('error: ', err) })
        } else console.log('No media devices found')

        AWS.config.region = 'us-east-1'; // Region
        AWS.config.credentials = new AWS.CognitoIdentityCredentials({
            IdentityPoolId: 'us-east-1:0454d1fd-9fc8-4eac-bd84-9489427fdf0b'
        })
        self.s3 = new AWS.S3()
    },
    methods: {
        capture () {
            var self = this
            self.canvas = self.$refs.canvas
            var context = self.canvas.getContext("2d").drawImage(self.video, 0, 0, 640, 480)
            console.log('Took a picture')
            self.save_to_s3()
        },
        save_to_s3 () {
            var self = this
            self.canvas.toBlob((blob) => {
                var key = Math.random().toString(36).substr(-10) + '.png'
                var params = {Bucket: BUCKET, Key: key, Body: blob}
                self.s3.upload(params, (err, d) => {
                    if (err) console.log('Failed uploading to S3 - ', err)
                    else {
                        console.log('Picture uploaded')
                        self.captures.push({name: key, src: self.canvas.toDataURL("image/png")})
                    }
                })
            }, "image/png")
        }
    }
})