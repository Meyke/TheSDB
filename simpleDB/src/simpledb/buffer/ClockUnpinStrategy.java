package simpledb.buffer;

/**
 * Clock. Fa una scansione, come nel caso naif, ma non dall'inizio, bensi' dalla pagina 
 * successiva a quella del rimpiazzo precedente.
 * @author micheletedesco1
 *
 */
public class ClockUnpinStrategy implements ChooseUnpinnedBufferStrategy{

	private  Buffer[] bufferpool;
	private int currentPosition; // posizione corrente del clock


	public ClockUnpinStrategy(Buffer[] bufferpool) {
		super();
		this.bufferpool = bufferpool;
		this.currentPosition = 0;
	}

	@Override
	public Buffer chooseUnpinnedBuffer() {
		Buffer buff = null;
		int tries = 0; //numeri di tentativi per trovare la pagina unpinned (utile per evitare di ciclare indefinitivamente nel buffer).
		int i = currentPosition; //da questa posizione cerco la prossima pagina libera
		while (tries < this.bufferpool.length) { 
			if (i == this.bufferpool.length-1) //se ho raggiunto la fine del buffer, ricomincio dall'inizio
				i = 0;
			buff = this.bufferpool[i];
			if (!buff.isPinned()) {
				currentPosition = i+1;
				return buff;
			}
			i++;
		}	
		return null;
	}



}
