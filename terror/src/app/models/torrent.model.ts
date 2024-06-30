export interface Torrent {
    id: string;
    name: string;
    size: number;
    progress: number;
    status: 'downloading' | 'seeding' | 'paused' | 'completed';
    downloadSpeed: number;
    uploadSpeed: number;
    peers: number;
    dateAdded: Date;
  }