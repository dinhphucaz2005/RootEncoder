/*
 * Copyright (C) 2024 pedroSG94.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.pedro.rtmp.rtmp

import android.util.Base64
import android.util.Log
import com.pedro.common.AudioCodec
import com.pedro.common.ConnectChecker
import com.pedro.common.VideoCodec
import com.pedro.common.base.BaseSender
import com.pedro.common.frame.MediaFrame
import com.pedro.common.onMainThread
import com.pedro.common.validMessage
import com.pedro.rtmp.flv.BasePacket
import com.pedro.rtmp.flv.FlvPacket
import com.pedro.rtmp.flv.FlvType
import com.pedro.rtmp.flv.audio.packet.AacPacket
import com.pedro.rtmp.flv.audio.packet.G711Packet
import com.pedro.rtmp.flv.video.packet.Av1Packet
import com.pedro.rtmp.flv.video.packet.H264Packet
import com.pedro.rtmp.flv.video.packet.H265Packet
import com.pedro.rtmp.utils.socket.RtmpSocket
import kotlinx.coroutines.isActive
import kotlinx.coroutines.runInterruptible
import java.nio.ByteBuffer

/**
 * Created by pedro on 8/04/21.
 */
class RtmpSender(
  connectChecker: ConnectChecker,
  private val commandsManager: CommandsManager,
) : BaseSender(connectChecker, "RtmpSender") {

  private var audioPacket: BasePacket = AacPacket()
  private var videoPacket: BasePacket = H264Packet()
  var socket: RtmpSocket? = null

  override fun setVideoInfo(sps: ByteBuffer, pps: ByteBuffer?, vps: ByteBuffer?) {
    fun printBytes(data: ByteBuffer, tag: String) {
      val temp = data.duplicate()
      val byteArray = ByteArray(temp.remaining())
      temp.get(byteArray)
      fun extractSpsRbsp(bytes: ByteArray): ByteArray {
        val offset = (
          if (bytes.size >= 4 &&
            bytes[0] == 0.toByte() &&
            bytes[1] == 0.toByte() &&
            bytes[2] == 0.toByte() &&
            bytes[3] == 1.toByte()
          ) 4 else if (bytes.size >= 3 &&
            bytes[0] == 0.toByte() &&
            bytes[1] == 0.toByte() &&
            bytes[2] == 1.toByte()
          ) 3 else 0) + 1 // skip nal unit type

        return bytes.copyOfRange(offset, bytes.size)
      }

      val base64String = Base64.encodeToString(extractSpsRbsp(byteArray), Base64.NO_WRAP)
      Log.d(tag, base64String)
      Log.d(tag, byteArray.joinToString(separator = "") { String.format("%02X", it) })
    }
    videoPacket = when (commandsManager.videoCodec) {
      VideoCodec.H265 -> {
        if (vps == null || pps == null) throw IllegalArgumentException("pps or vps can't be null with h265")
        printBytes(data = sps, tag = "SPS-H265")
        printBytes(data = pps, tag = "PPS-H265")
        printBytes(data = vps, tag = "VPS-H265")
        H265Packet().apply { sendVideoInfo(sps, pps, vps) }
      }

      VideoCodec.AV1 -> {
        printBytes(data = sps, tag = "SPS-AV1")
        Av1Packet().apply { sendVideoInfo(sps) }
      }

      else -> {
        if (pps == null) throw IllegalArgumentException("pps can't be null with h264")
        printBytes(data = sps, tag = "SPS-H264")
        printBytes(data = pps, tag = "PPS-H264")

        H264Packet().apply { sendVideoInfo(sps, pps) }
      }
    }
  }

  override fun setAudioInfo(sampleRate: Int, isStereo: Boolean) {
    audioPacket = when (commandsManager.audioCodec) {
      AudioCodec.G711 -> G711Packet().apply { sendAudioInfo() }
      AudioCodec.AAC -> AacPacket().apply { sendAudioInfo(sampleRate, isStereo) }
      AudioCodec.OPUS -> {
        throw IllegalArgumentException("Unsupported codec: ${commandsManager.audioCodec.name}")
      }
    }
  }

  override suspend fun onRun() {
    while (scope.isActive && running) {
      val error = runCatching {
        val mediaFrame = runInterruptible { queue.take() }
        getFlvPacket(mediaFrame) { flvPacket ->
          var size = 0
          if (flvPacket.type == FlvType.VIDEO) {
            videoFramesSent++
            socket?.let { socket ->
              size = commandsManager.sendVideoPacket(flvPacket, socket)
              if (isEnableLogs) {
                Log.i(TAG, "wrote Video packet, size $size")
              }
            }
          } else {
            audioFramesSent++
            socket?.let { socket ->
              size = commandsManager.sendAudioPacket(flvPacket, socket)
              if (isEnableLogs) {
                Log.i(TAG, "wrote Audio packet, size $size")
              }
            }
          }
          bytesSend += size
          bytesSendPerSecond += size
        }
      }.exceptionOrNull()
      if (error != null) {
        onMainThread {
          connectChecker.onConnectionFailed("Error send packet, ${error.validMessage()}")
        }
        Log.e(TAG, "send error: ", error)
        running = false
        return
      }
    }
  }

  override suspend fun stopImp(clear: Boolean) {
    audioPacket.reset(clear)
    videoPacket.reset(clear)
  }

//  val job = CoroutineScope(Dispatchers.IO) + SupervisorJob()

  private suspend fun getFlvPacket(mediaFrame: MediaFrame?, callback: suspend (FlvPacket) -> Unit) {
    if (mediaFrame == null) return
    if (mediaFrame.info.isKeyFrame) {
      fun skipHeader(buffer: ByteBuffer, headerSize: Int = 0xd0): ByteBuffer {
        val dup = buffer.duplicate()
        require(dup.remaining() >= headerSize)
        dup.position(dup.position() + headerSize)
        return dup.slice()
      }

      val payload = skipHeader(mediaFrame.data)
      val newMediaFrame = MediaFrame(
        data = payload, info = mediaFrame.info, type = mediaFrame.type
      )
      when (mediaFrame.type) {
        MediaFrame.Type.VIDEO -> videoPacket.createFlvPacket(newMediaFrame) { callback(it) }
        MediaFrame.Type.AUDIO -> audioPacket.createFlvPacket(newMediaFrame) { callback(it) }
      }
      return
    }
//    if (mediaFrame.info.isKeyFrame) {
//      job.launch {
//        val file =
//          File("/data/data/app.smartsports.sst.vn.dev/cache/${System.currentTimeMillis()}_frame.h264")
//        file.outputStream().use { output ->
//          val temp = mediaFrame.data.duplicate()
//          val byteArray = ByteArray(temp.remaining())
//          temp.get(byteArray)
//          output.write(byteArray)
//        }
//      }
//    }
    when (mediaFrame.type) {
      MediaFrame.Type.VIDEO -> videoPacket.createFlvPacket(mediaFrame) { callback(it) }
      MediaFrame.Type.AUDIO -> audioPacket.createFlvPacket(mediaFrame) { callback(it) }
    }
  }
}