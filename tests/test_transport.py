"""L1 transport layer: MessagePack over UDS — encode/decode, round-trip,
large payloads, JSON fallback, connection lifecycle."""

import asyncio
import json
import unittest
from pathlib import Path

from zcode_supervisor import transport as T


class EnvelopeCodec(unittest.TestCase):
    def test_round_trip_all_types(self):
        for msg_type, payload in [
            (T.MSG_DISPATCH, {"task_id": "t1", "prompt": "hello"}),
            (T.MSG_PROGRESS, {"state": "running", "age_s": 1.5}),
            (T.MSG_RESULT, {"outcome": "delivered", "files": ["a.py"]}),
            (T.MSG_CONTROL, {"action": "stop"}),
        ]:
            env = T.MessageEnvelope(msg_type, payload)
            raw = T.encode_envelope(env)
            decoded = T.decode_envelope(raw)
            self.assertEqual(decoded.msg_type, msg_type)
            self.assertEqual(decoded.payload, payload)

    def test_unicode_payload(self):
        env = T.MessageEnvelope(T.MSG_DISPATCH,
                                {"prompt": "实现功能：中文字符串测试",
                                 "注释": "日本語テキスト"})
        raw = T.encode_envelope(env)
        decoded = T.decode_envelope(raw)
        self.assertEqual(decoded.payload["prompt"], "实现功能：中文字符串测试")

    def test_large_payload(self):
        big = {"data": "x" * 1_000_000}
        env = T.MessageEnvelope(T.MSG_PROGRESS, big)
        raw = T.encode_envelope(env)
        decoded = T.decode_envelope(raw)
        self.assertEqual(len(decoded.payload["data"]), 1_000_000)

    def test_reject_unknown_type(self):
        with self.assertRaises(T.TransportError):
            T.MessageEnvelope(99, {})

    def test_reject_oversized(self):
        env = T.MessageEnvelope(T.MSG_PROGRESS,
                                {"data": "y" * (T._MAX_PAYLOAD + 1)})
        with self.assertRaises(T.TransportError):
            T.encode_envelope(env)

    def test_json_fallback(self):
        original = T._HAVE_MSGPACK
        T._HAVE_MSGPACK = False
        try:
            env = T.MessageEnvelope(T.MSG_DISPATCH, {"key": "value", "n": 42})
            raw = T.encode_envelope(env)
            self.assertEqual(raw[0], T.MSG_DISPATCH)
            payload = json.loads(raw[1:].decode("utf-8"))
            self.assertEqual(payload, {"key": "value", "n": 42})
            decoded = T.decode_envelope(raw)
            self.assertEqual(decoded.payload, {"key": "value", "n": 42})
        finally:
            T._HAVE_MSGPACK = original


class UdsRoundTrip(unittest.TestCase):
    """End-to-end UDS socket round-trip — all in one event loop."""

    def _scenario(self, body):
        async def run():
            server = T.UdsTransport()
            await server.listen()
            await asyncio.sleep(0.02)
            client = T.UdsTransport(socket_path=server.path)
            await client.connect()
            await asyncio.sleep(0.05)
            try:
                result = await body(client, server)
            finally:
                await client.close()
                await server.close()
            return result
        return asyncio.run(run())

    def test_basic_round_trip(self):
        async def body(client, server):
            await client.send(T.MessageEnvelope(
                T.MSG_DISPATCH, {"task_id": "rt-1", "prompt": "go"}))
            return await server.recv(timeout=5)
        received = self._scenario(body)
        self.assertEqual(received.msg_type, T.MSG_DISPATCH)
        self.assertEqual(received.payload["task_id"], "rt-1")

    def test_bidirectional(self):
        async def body(client, server):
            await client.send(T.MessageEnvelope(
                T.MSG_DISPATCH, {"seq": 1}))
            got = await server.recv(timeout=5)
            await server.send(T.MessageEnvelope(
                T.MSG_RESULT, {"outcome": "ok", "seq": 1}))
            reply = await client.recv(timeout=5)
            return got, reply
        got, reply = self._scenario(body)
        self.assertEqual(got.payload["seq"], 1)
        self.assertEqual(reply.payload["outcome"], "ok")

    def test_many_messages(self):
        async def body(client, server):
            for i in range(100):
                await client.send(T.MessageEnvelope(
                    T.MSG_PROGRESS, {"seq": i, "detail": f"step {i}"}))
            received = []
            for _ in range(100):
                env = await server.recv(timeout=5)
                received.append(env.payload["seq"])
            return received, client.stats(), server.stats()
        seqs, cstats, sstats = self._scenario(body)
        self.assertEqual(seqs, list(range(100)))
        self.assertEqual(cstats["send_count"], 100)
        self.assertEqual(sstats["recv_count"], 100)

    def test_throughput(self):
        import time
        async def body(client, server):
            started = time.perf_counter()
            for i in range(1000):
                await client.send(T.MessageEnvelope(
                    T.MSG_DISPATCH, {"seq": i}))
                await server.recv(timeout=5)
            return time.perf_counter() - started, client.stats()
        elapsed, stats = self._scenario(body)
        self.assertLess(elapsed, 2.0, f"1000 RTT took {elapsed:.3f}s")
        self.assertEqual(stats["send_count"], 1000)

    def test_socket_cleanup(self):
        async def body(client, server):
            return server.path
        path = self._scenario(body)
        self.assertFalse(Path(path).exists())


if __name__ == "__main__":
    unittest.main()
