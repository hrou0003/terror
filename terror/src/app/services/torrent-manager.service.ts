import { Injectable } from '@angular/core';
import { Observable, of, BehaviorSubject } from 'rxjs';
import { Torrent } from '../models/torrent.model';

@Injectable({
  providedIn: 'root'
})
export class TorrentManagerService {
  private torrents: Torrent[] = [
    {
      id: '1',
      name: 'Ubuntu 22.04 LTS',
      size: 3_000_000_000,
      progress: 65,
      status: 'downloading',
      downloadSpeed: 2_500_000,
      uploadSpeed: 500_000,
      peers: 42,
      dateAdded: new Date('2024-06-29T10:30:00')
    },
    {
      id: '2',
      name: 'Debian 11',
      size: 2_500_000_000,
      progress: 100,
      status: 'seeding',
      downloadSpeed: 0,
      uploadSpeed: 1_500_000,
      peers: 15,
      dateAdded: new Date('2024-06-28T14:15:00')
    }
  ];

  private torrentsSubject = new BehaviorSubject<Torrent[]>(this.torrents);

  constructor() {
    // Simulate progress updates
    setInterval(() => this.updateTorrents(), 1000);
  }

  getTorrents(): Observable<Torrent[]> {
    return this.torrentsSubject.asObservable();
  }

  addTorrent(magnetLink: string): Observable<boolean> {
    // Simulate adding a new torrent
    const newTorrent: Torrent = {
      id: (this.torrents.length + 1).toString(),
      name: `New Torrent ${this.torrents.length + 1}`,
      size: Math.floor(Math.random() * 5_000_000_000),
      progress: 0,
      status: 'downloading',
      downloadSpeed: 0,
      uploadSpeed: 0,
      peers: 0,
      dateAdded: new Date()
    };

    this.torrents.push(newTorrent);
    this.torrentsSubject.next(this.torrents);
    return of(true);
  }

  pauseTorrent(id: string): Observable<boolean> {
    const torrent = this.torrents.find(t => t.id === id);
    if (torrent) {
      torrent.status = 'paused';
      this.torrentsSubject.next(this.torrents);
    }
    return of(!!torrent);
  }

  resumeTorrent(id: string): Observable<boolean> {
    const torrent = this.torrents.find(t => t.id === id);
    if (torrent) {
      torrent.status = torrent.progress === 100 ? 'seeding' : 'downloading';
      this.torrentsSubject.next(this.torrents);
    }
    return of(!!torrent);
  }

  removeTorrent(id: string): Observable<boolean> {
    const index = this.torrents.findIndex(t => t.id === id);
    if (index !== -1) {
      this.torrents.splice(index, 1);
      this.torrentsSubject.next(this.torrents);
      return of(true);
    }
    return of(false);
  }

  private updateTorrents() {
    this.torrents.forEach(torrent => {
      if (torrent.status === 'downloading') {
        torrent.progress = Math.min(100, torrent.progress + Math.random() * 5);
        torrent.downloadSpeed = Math.floor(Math.random() * 5_000_000);
        torrent.uploadSpeed = Math.floor(Math.random() * 1_000_000);
        torrent.peers = Math.floor(Math.random() * 50) + 1;

        if (torrent.progress === 100) {
          torrent.status = 'completed';
        }
      } else if (torrent.status === 'seeding') {
        torrent.uploadSpeed = Math.floor(Math.random() * 2_000_000);
        torrent.peers = Math.floor(Math.random() * 20) + 1;
      }
    });

    this.torrentsSubject.next(this.torrents);
  }
}