package kafka.server.epoch

import java.io.File

import kafka.cluster.{Partition, Replica}
import org.apache.kafka.common.record.ByteBufferLogInputStream.ByteBufferLogEntry

object LeaderEpoch{
  val LeaderEpochCheckpointFilename = "leader-epoch-checkpoint"
}

trait EpochAction {
  def onMessage(entry: ByteBufferLogEntry)
}

object DummyEpochAction extends EpochAction{
  override def onMessage(entry: ByteBufferLogEntry): Unit ={}
}

//Used by the leader to stamp the epoch on messages.
class EpochOverwriteAction(epoch: Int) extends EpochAction {
  def onMessage(entry: ByteBufferLogEntry) = {
    //stamp epoch on message
    entry.setLeaderEpoch(epoch)
  }
}

//Used by the follower to track if the leader's epoch changed.
class EpochChangedAction(tracker: LeaderEpochTracker) extends EpochAction{
  override def onMessage(entry: ByteBufferLogEntry): Unit = {
    //Check to see if epoch has changed, record it if it has
    val epochOnReplicatedMessage = entry.record.leaderEpoch()
    if(epochOnReplicatedMessage > tracker.epoch())
      tracker.appendEpoch(epochOnReplicatedMessage, entry.offset())
  }
}
case class EpochEntry(epoch: Int, startOffset: Long)

class LeaderEpochTracker(partition: Partition, dir: File, replica: Replica) {
  private val checkpoint = new LeaderEpochCheckpoint(new File(dir, LeaderEpoch.LeaderEpochCheckpointFilename))

  var epochs = Seq[EpochEntry]()

  def flush(): Unit = {
    checkpoint.write(epochs)
  }

  def load(): Unit = {
    epochs = checkpoint.read()
  }

  def becomeFollower(leaderEpoch: Int) = {
    updateWithLeo(leaderEpoch)
  }

  def becomeLeader(leaderEpoch: Int) = {
    updateWithLeo(leaderEpoch)
  }

  private def updateWithLeo(leaderEpoch: Int) = {
    appendEpoch(leaderEpoch, replica.logEndOffset.messageOffset)
  }

  def appendEpoch(leaderEpoch: Int, offset: Long): Unit = {
    println("Epoch was updated to "+ leaderEpoch)
    epochs :+ EpochEntry(leaderEpoch, offset)
    flush()
  }

  def resetTo(leaderEpoch: Int, offset: Long): Unit = {
    //are there older offsets?
    //if so delete all older offsets and take this one. flush file.
    if(epochs.last.startOffset > offset){
      epochs = epochs.filter(entry => entry.startOffset > offset)
      flush()
    }
  }

  def epoch(): Int = {
    epochs.last.epoch
  }

}
