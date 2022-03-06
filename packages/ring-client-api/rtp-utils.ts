import { logDebug, logError } from './util'
import { SrtpOptions } from '@homebridge/camera-utils'
import { DingKind } from './ring-types'
import dgram from 'dgram';
const stun = require('stun')

const stunMagicCookie = 0x2112a442 // https://tools.ietf.org/html/rfc5389#section-6

export interface RtpStreamOptions extends SrtpOptions {
  port: number
  rtcpPort: number
}

export interface RtpOptions {
  audio: RtpStreamOptions
  video: RtpStreamOptions
}

export interface RtpStreamDescription extends RtpStreamOptions {
  ssrc?: number
  iceUFrag?: string
  icePwd?: string
}

export interface RtpDescription {
  address: string
  audio: RtpStreamDescription
  video: RtpStreamDescription
  sdp: string
}

export function isStunMessage(message: Buffer) {
  return message.length > 8 && message.readInt32BE(4) === stunMagicCookie
}

export function sendStunBindingRequest({
  rtpDescription,
  rtpSplitter,
  rtcpSplitter,
  localUfrag,
  type,
}: {
  rtpSplitter: dgram.Socket
  rtcpSplitter: dgram.Socket
  rtpDescription: RtpDescription
  localUfrag?: string
  type: 'video' | 'audio'
}) {
  const message = stun.createMessage(1),
    remoteDescription = rtpDescription[type],
    { address } = rtpDescription,
    { iceUFrag, icePwd, port, rtcpPort } = remoteDescription

  if (iceUFrag && icePwd && localUfrag) {
    // Full ICE supported.  Send as formal stun request
    message.addUsername(iceUFrag + ':' + localUfrag)
    message.addMessageIntegrity(icePwd)

    stun
      .request(`${address}:${port}`, {
        socket: rtpSplitter,
        message,
      })
      .then(() => logDebug(`${type} stun complete`))
      .catch((e: Error) => {
        logError(`${type} stun error`)
        logError(e)
      })
  } else {
    // ICE not supported.  Fire and forget the stun request for RTP and RTCP
    const encodedMessage = stun.encode(message)
    try {
      rtpSplitter.send(encodedMessage, port, address)
    }
    catch (e) {
      logError(e);
    }

    try {
      rtcpSplitter.send(encodedMessage, rtcpPort, address);
    }
    catch (e) {
      logError(e);
    }
  }
}

export function createStunResponder(rtpSplitter: dgram.Socket) {
  return rtpSplitter.on('message', (message, info) => {
    if (!isStunMessage(message)) {
      return null
    }

    try {
      const decodedMessage = stun.decode(message),
        response = stun.createMessage(
          stun.constants.STUN_BINDING_RESPONSE,
          decodedMessage.transactionId
        )

      response.addXorAddress(info.address, info.port)
      try {
        rtpSplitter.send(stun.encode(response), info.port, info.address)
      }
      catch (e) {
        logError(e);
      }
    } catch (e) {
      logDebug('Failed to Decode STUN Message')
      logDebug(message.toString('hex'))
      logDebug(e)
    }

    return null
  })
}

export interface LiveCallSession {
  alexa_port: 18443
  app_session_token: string
  availability_zone: 'availability-zone'
  custom_timer: { max_sec: number }
  ding_id: string
  ding_kind: DingKind
  doorbot_id: number
  exp: number
  ip: string
  port: number
  private_ip: string
  rms_fqdn: string
  rms_version: string
  rsp_port: number
  rtsp_port: number
  session_id: string
  sip_port: number
  webrtc_port: number
  webrtc_url: string
  wwr_port: number
}

export function parseLiveCallSession(sessionId: string) {
  const encodedSession = sessionId.split('.')[1],
    buff = Buffer.from(encodedSession, 'base64'),
    text = buff.toString('ascii')
  return JSON.parse(text) as LiveCallSession
}
