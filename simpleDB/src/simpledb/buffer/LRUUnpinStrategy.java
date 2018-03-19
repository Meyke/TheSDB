package simpledb.buffer;

public class LRUUnpinStrategy implements ChooseUnpinnedBufferStrategy{

	private  Buffer[] bufferpool;
	
	public LRUUnpinStrategy(Buffer[] bufferpool) {
		this.bufferpool = bufferpool;
	}

	@Override
	public Buffer chooseUnpinnedBuffer() {
		long lastUnpinned = Long.MAX_VALUE;
		Buffer bufferScelto = null;
		for (int i = 0; i < bufferpool.length; i++) {
			if (!bufferpool[i].isPinned() && bufferpool[i].getLastUnpinTimestamp()<lastUnpinned) {
				bufferScelto = bufferpool[i];
				lastUnpinned = bufferpool[i].getLastUnpinTimestamp();
			}
		}
		return bufferScelto;
	}
	

}
