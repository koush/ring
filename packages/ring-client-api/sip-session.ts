import { reservePorts } from '@homebridge/camera-utils'
import dgram from 'dgram'
import { ReplaySubject, timer } from 'rxjs'
import { RingCamera } from './ring-camera'
import {
  createStunResponder,
  RtpDescription,
  RtpOptions,
  sendStunBindingRequest,
} from './rtp-utils'
import { expiredDingError, SipCall, SipOptions } from './sip-call'
import { Subscribed } from './subscribed'
import { logDebug, logError } from './util'

export class SipSession extends Subscribed {
  private hasStarted = false
  private hasCallEnded = false
  private onCallEndedSubject = new ReplaySubject(1)
  private sipCall: SipCall
  onCallEnded = this.onCallEndedSubject.asObservable()

  constructor(
    public readonly sipOptions: SipOptions,
    public readonly rtpOptions: RtpOptions,
    public readonly audioSplitter: dgram.Socket,
    public audioRtcpSplitter: dgram.Socket,
    public readonly videoSplitter: dgram.Socket,
    public videoRtcpSplitter: dgram.Socket,
    private readonly tlsPort: number,
    public readonly camera: RingCamera
  ) {
    super()

    this.sipCall = this.createSipCall(this.sipOptions)
  }

  createSipCall(sipOptions: SipOptions) {
    if (this.sipCall) {
      this.sipCall.destroy()
    }

    const call = (this.sipCall = new SipCall(
      sipOptions,
      this.rtpOptions,
      this.tlsPort
    ))

    this.addSubscriptions(
      call.onEndedByRemote.subscribe(() => this.callEnded(false))
    )

    return this.sipCall
  }

  async start(): Promise<RtpDescription> {
    if (this.hasStarted) {
      throw new Error('SIP Session has already been started')
    }
    this.hasStarted = true

    if (this.hasCallEnded) {
      throw new Error('SIP Session has already ended')
    }

    try {
      const rtpDescription = await this.sipCall.invite(),
        sendStunRequests = () => {
          sendStunBindingRequest({
            rtpSplitter: this.audioSplitter,
            rtcpSplitter: this.audioRtcpSplitter,
            rtpDescription,
            localUfrag: this.sipCall.audioUfrag,
            type: 'audio',
          })
          sendStunBindingRequest({
            rtpSplitter: this.videoSplitter,
            rtcpSplitter: this.videoRtcpSplitter,
            rtpDescription,
            localUfrag: this.sipCall.videoUfrag,
            type: 'video',
          })
        }

      // if rtcp-mux is supported, rtp splitter will be used for both rtp and rtcp
      if (rtpDescription.audio.port === rtpDescription.audio.rtcpPort) {
        this.audioRtcpSplitter.close()
        this.audioRtcpSplitter = this.audioSplitter
      }
      if (rtpDescription.video.port === rtpDescription.video.rtcpPort) {
        this.videoRtcpSplitter.close()
        this.videoRtcpSplitter = this.videoSplitter
      }

      if (rtpDescription.video.iceUFrag) {
        // ICE is supported
        logDebug(`Connecting to ${this.camera.name} using ICE`)
        createStunResponder(this.audioSplitter)
        createStunResponder(this.videoSplitter)

        sendStunRequests()
      } else {
        // ICE is not supported, use stun as keep alive
        logDebug(`Connecting to ${this.camera.name} using STUN`)
        this.addSubscriptions(
          // hole punch every .5 seconds to keep stream alive and port open (matches behavior from Ring app)
          timer(0, 500).subscribe(sendStunRequests)
        )
      }

      this.audioSplitter.once('message', () => {
        logDebug(`Audio stream latched for ${this.camera.name}`)
      })
      this.videoSplitter.once('message', () => {
        logDebug(`Video stream latched for ${this.camera.name}`)
      })

      return rtpDescription
    } catch (e) {
      if (e === expiredDingError) {
        const sipOptions = await this.camera.getUpdatedSipOptions(
          this.sipOptions.dingId
        )
        this.createSipCall(sipOptions)
        this.hasStarted = false
        return this.start()
      }

      this.callEnded(true)
      throw e
    }
  }

  async reservePort(bufferPorts = 0) {
    const ports = await reservePorts({ count: bufferPorts + 1 })
    return ports[0]
  }

  requestKeyFrame() {
    return this.sipCall.requestKeyFrame()
  }

  activateCameraSpeaker() {
    return this.sipCall.activateCameraSpeaker()
  }

  private callEnded(sendBye: boolean) {
    if (this.hasCallEnded) {
      return
    }
    this.hasCallEnded = true

    if (sendBye) {
      this.sipCall.sendBye().catch(logError)
    }

    // clean up
    this.onCallEndedSubject.next(null)
    this.sipCall.destroy()
    this.videoSplitter.close()
    this.audioSplitter.close()
    this.unsubscribe()
  }

  stop() {
    this.callEnded(true)
  }
}
