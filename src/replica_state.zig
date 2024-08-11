const Connection = @import("connection.zig").Connection;

pub const ReplconfCapability = enum {
    psync2,
};

const InitialPing = struct {};
// TODO remove listening_port where it's not needed (?).
const FirstReplconf = struct { listening_port: u16 };
const SecondReplconf = struct { listening_port: u16, capa: ReplconfCapability };
const ReceivingSync = struct { listening_port: u16, capa: ReplconfCapability };
const ConnectedReplica = struct { listening_port: u16, capa: ReplconfCapability };
pub const ReplicaStateType = enum {
    initial_ping,
    first_replconf,
    second_replconf,
    receiving_sync,
    connected_replica,
};
pub fn isReplicaReadyToReceive(replica_state: ReplicaState) bool {
    switch (replica_state) {
        .receiving_sync, .connected_replica => true,
        else => false,
    }
}
/// A replica performs a handshake with the master server by going through these states in this order (no skipping!):
/// InitialPing <-- after a replica sends a PING to the master.
/// FirstReplconf <-- after a replica sends the first "REPLCONF listening-port <port>" command to the master.
/// SecondReplconf <-- after a replica sends "REPLCONF capa psync2" or a similar command to the master.
/// ReceivingSync <-- after a replica sends "PSYNC ? -1" to the master, the master replies with "+FULLRESYNC <replid> 0". The
///     master should start sending the RDB file now.
/// ConnectedReplica <-- Once the RDB file is sent over, the replica is now fully synchronized and connected. The master will relay
///     write commands to it.
pub const ReplicaState = union(ReplicaStateType) {
    initial_ping: InitialPing,
    first_replconf: FirstReplconf,
    second_replconf: SecondReplconf,
    receiving_sync: ReceivingSync,
    connected_replica: ConnectedReplica,
};
