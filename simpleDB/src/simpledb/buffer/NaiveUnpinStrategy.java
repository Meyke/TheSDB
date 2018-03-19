package simpledb.buffer;

public class NaiveUnpinStrategy implements ChooseUnpinnedBufferStrategy{
	
	private  Buffer[] bufferpool;

	public NaiveUnpinStrategy(Buffer[] bufferpool) {		
		this.bufferpool = bufferpool;
	}

	@Override
	public Buffer chooseUnpinnedBuffer() {
		for (Buffer buff : bufferpool)
			if (!buff.isPinned())
				return buff;
		return null;
	}
	
	
	
	
	

}
