import { TestBed } from '@angular/core/testing';

import { TorrentManagerService } from './torrent-manager.service';

describe('TorrentManagerService', () => {
  let service: TorrentManagerService;

  beforeEach(() => {
    TestBed.configureTestingModule({});
    service = TestBed.inject(TorrentManagerService);
  });

  it('should be created', () => {
    expect(service).toBeTruthy();
  });
});
