import { SelectionModel } from '@angular/cdk/collections';
import { CommonModule, DatePipe, DecimalPipe, TitleCasePipe } from '@angular/common';
import { Component, TrackByFunction, computed, effect, signal } from '@angular/core';
import { toObservable, toSignal } from '@angular/core/rxjs-interop';
import { FormsModule } from '@angular/forms';
import { lucideArrowUpDown, lucideChevronDown, lucideMoreHorizontal } from '@ng-icons/lucide';
import { HlmButtonDirective, HlmButtonModule } from '@spartan-ng/ui-button-helm';
import { HlmCheckboxCheckIconComponent, HlmCheckboxComponent } from '@spartan-ng/ui-checkbox-helm';
import { HlmIconComponent, provideIcons } from '@spartan-ng/ui-icon-helm';
import { HlmInputDirective } from '@spartan-ng/ui-input-helm';
import { BrnMenuTriggerDirective } from '@spartan-ng/ui-menu-brain';
import { HlmMenuItemCheckboxDirective, HlmMenuItemDirective, HlmMenuModule } from '@spartan-ng/ui-menu-helm';
import { BrnHeaderDefDirective, BrnTableModule, PaginatorState, useBrnColumnManager } from '@spartan-ng/ui-table-brain';
import { HlmTableModule } from '@spartan-ng/ui-table-helm';
import { BrnSelectModule } from '@spartan-ng/ui-select-brain';
import { HlmSelectModule } from '@spartan-ng/ui-select-helm';
import { debounceTime, map } from 'rxjs';
import { Torrent } from 'src/app/models/torrent.model';
import { TorrentManagerService } from 'src/app/services/torrent-manager.service';
import { BrnAlertDialogContentDirective } from '@spartan-ng/ui-alertdialog-brain';
import { hlmMuted } from '@spartan-ng/ui-typography-helm';

@Component({
  selector: 'torrent-table',
  standalone: true,
  imports: [
    FormsModule,
    BrnMenuTriggerDirective,
    HlmMenuModule,
    HlmButtonDirective,
    BrnTableModule,
    HlmTableModule,
    HlmMenuItemDirective,
    HlmMenuItemCheckboxDirective,
    BrnAlertDialogContentDirective,
    HlmButtonModule,
    DecimalPipe,
    BrnHeaderDefDirective,
    TitleCasePipe,
    DatePipe,
    HlmIconComponent,
    HlmInputDirective,
    HlmCheckboxCheckIconComponent,
    HlmCheckboxComponent,
    BrnSelectModule,
    HlmSelectModule,
    CommonModule
  ],
  providers: [provideIcons({ lucideChevronDown, lucideMoreHorizontal, lucideArrowUpDown })],
  host: {
    class: 'w-full',
  },
  template: `
    <div class="flex flex-col justify-between gap-4 sm:flex-row">
      <input
        hlmInput
        class="w-full md:w-80"
        placeholder="Filter torrents..."
        [ngModel]="_nameFilter()"
        (ngModelChange)="_rawFilterInput.set($event)"
      />

      <button hlmBtn variant="outline" align="end" [brnMenuTriggerFor]="menu">
        Columns
        <hlm-icon name="lucideChevronDown" class="ml-2" size="sm" />
      </button>
      <ng-template #menu>
        <hlm-menu class="w-32">
          @for (column of _brnColumnManager.allColumns; track column.name) {
            <button
              hlmMenuItemCheckbox
              [disabled]="_brnColumnManager.isColumnDisabled(column.name)"
              [checked]="_brnColumnManager.isColumnVisible(column.name)"
              (triggered)="_brnColumnManager.toggleVisibility(column.name)"
            >
              <hlm-menu-item-check />
              <span>{{ column.label }}</span>
            </button>
          }
        </hlm-menu>
      </ng-template>
    </div>

    <brn-table
      hlm
      stickyHeader
      class="border-border mt-4 block h-[335px] overflow-auto rounded-md border"
      [dataSource]="_filteredSortedPaginatedTorrents()"
      [displayedColumns]="_allDisplayedColumns()"
      [trackBy]="_trackBy"
    >
      <brn-column-def name="select" class="w-12">
        <hlm-th *brnHeaderDef>
          <hlm-checkbox [checked]="_checkboxState()" (changed)="handleHeaderCheckboxChange()" />
        </hlm-th>
        <hlm-td *brnCellDef="let element">
          <hlm-checkbox [checked]="_isTorrentSelected(element)" (changed)="toggleTorrent(element)" />
        </hlm-td>
      </brn-column-def>
      <brn-column-def name="name" class="w-60 lg:flex-1">
        <hlm-th *brnHeaderDef>
          <button hlmBtn size="sm" variant="ghost" (click)="handleNameSortChange()">
            Name
            <hlm-icon class="ml-3" size="sm" name="lucideArrowUpDown" />
          </button>
        </hlm-th>
        <hlm-td truncate *brnCellDef="let element">{{ element.name }}</hlm-td>
      </brn-column-def>
      <brn-column-def name="size" class="w-32">
        <hlm-th *brnHeaderDef>Size</hlm-th>
        <hlm-td *brnCellDef="let element">{{ formatSize(element.size) }}</hlm-td>
      </brn-column-def>
      <brn-column-def name="progress" class="w-24">
        <hlm-th *brnHeaderDef>Progress</hlm-th>
        <hlm-td *brnCellDef="let element">{{ element.progress.toFixed(1) }}%</hlm-td>
      </brn-column-def>
      <brn-column-def name="status" class="w-32">
        <hlm-th *brnHeaderDef>Status</hlm-th>
        <hlm-td *brnCellDef="let element">{{ element.status | titlecase }}</hlm-td>
      </brn-column-def>
      <brn-column-def name="downloadSpeed" class="w-40">
        <hlm-th *brnHeaderDef>Download Speed</hlm-th>
        <hlm-td *brnCellDef="let element">{{ formatSpeed(element.downloadSpeed) }}</hlm-td>
      </brn-column-def>
      <brn-column-def name="uploadSpeed" class="w-40">
        <hlm-th *brnHeaderDef>Upload Speed</hlm-th>
        <hlm-td *brnCellDef="let element">{{ formatSpeed(element.uploadSpeed) }}</hlm-td>
      </brn-column-def>
      <brn-column-def name="peers" class="w-24">
        <hlm-th *brnHeaderDef>Peers</hlm-th>
        <hlm-td *brnCellDef="let element">{{ element.peers }}</hlm-td>
      </brn-column-def>
      <brn-column-def name="dateAdded" class="w-40">
        <hlm-th *brnHeaderDef>Date Added</hlm-th>
        <hlm-td *brnCellDef="let element">{{ element.dateAdded | date:'short' }}</hlm-td>
      </brn-column-def>
      <brn-column-def name="actions" class="w-16">
        <hlm-th *brnHeaderDef></hlm-th>
        <hlm-td *brnCellDef="let element">
          <button hlmBtn variant="ghost" class="h-6 w-6 p-0.5" align="end" [brnMenuTriggerFor]="menu">
            <hlm-icon class="w-4 h-4" name="lucideMoreHorizontal" />
          </button>

          <ng-template #menu>
            <hlm-menu>
              <hlm-menu-label>Actions</hlm-menu-label>
              <hlm-menu-separator />
              <hlm-menu-group>
                <button hlmMenuItem (click)="togglePause(element)">
                  {{ element.status === 'paused' ? 'Resume' : 'Pause' }}
                </button>
                <button hlmMenuItem (click)="deleteTorrent(element)">Delete</button>
              </hlm-menu-group>
            </hlm-menu>
          </ng-template>
        </hlm-td>
      </brn-column-def>
      <div class="flex items-center justify-center p-20 text-muted-foreground" brnNoDataRow>No torrents</div>
      <hlm-trow 
        *brnRowDef="let torrent"
        (click)="selectTorrent(torrent)"
        [class.bg-primary-100]="torrent.id === _selectedTorrent()?.id"
      ></hlm-trow>
    </brn-table>
    <div
      class="flex flex-col justify-between mt-4 sm:flex-row sm:items-center"
      *brnPaginator="let ctx; totalElements: _totalElements(); pageSize: _pageSize(); onStateChange: _onStateChange"
    >
      <span class="text-sm text-muted-foreground text-sm">{{ _selected().length }} of {{ _totalElements() }} torrent(s) selected</span>
      <div class="flex mt-2 sm:mt-0">
        <brn-select class="inline-block" placeholder="{{ _availablePageSizes[0] }}" [(ngModel)]="_pageSize">
          <hlm-select-trigger class="inline-flex mr-1 w-15 h-9">
            <hlm-select-value />
          </hlm-select-trigger>
          <hlm-select-content>
            @for (size of _availablePageSizes; track size) {
              <hlm-option [value]="size">
                {{ size === 10000 ? 'All' : size }}
              </hlm-option>
            }
          </hlm-select-content>
        </brn-select>

        <div class="flex space-x-1">
          <button size="sm" variant="outline" hlmBtn [disabled]="!ctx.decrementable()" (click)="ctx.decrement()">
            Previous
          </button>
          <button size="sm" variant="outline" hlmBtn [disabled]="!ctx.incrementable()" (click)="ctx.increment()">
            Next
          </button>
        </div>
      </div>
    </div>
    <div 
      class="fixed bottom-0 left-0 right-0 bg-white border-t border-gray-200 transition-transform duration-300 ease-in-out transform"
      [class.translate-y-full]="!_selectedTorrent()"
      [class.translate-y-0]="_selectedTorrent()"
      style="height: 300px; overflow-y: auto;"
    >
      <div class="p-4" *ngIf="_selectedTorrent() as torrent">
        <div class="flex justify-between items-center mb-4">
          <h2 class="text-xl font-bold">{{ torrent.name }}</h2>
          <button hlmBtn variant="ghost" size="icon" (click)="closePanel()">
            <hlm-icon name="lucideX" />
          </button>
        </div>
        <div class="grid grid-cols-2 gap-4">
          <div>
            <p [class]="hlmMuted">Size: {{ formatSize(torrent.size) }}</p>
            <p [class]="hlmMuted">Progress: {{ torrent.progress.toFixed(1) }}%</p>
            <p [class]="hlmMuted">Status: {{ torrent.status | titlecase }}</p>
            <p [class]="hlmMuted">Peers: {{ torrent.peers }}</p>
          </div>
          <div>
            <p [class]="hlmMuted">Download Speed: {{ formatSpeed(torrent.downloadSpeed) }}</p>
            <p [class]="hlmMuted">Upload Speed: {{ formatSpeed(torrent.uploadSpeed) }}</p>
            <p [class]="hlmMuted">Date Added: {{ torrent.dateAdded | date:'medium' }}</p>
          </div>
        </div>
        <div class="mt-4">
          <button hlmBtn variant="outline" class="mr-2" (click)="togglePause(torrent)">
            {{ torrent.status === 'paused' ? 'Resume' : 'Pause' }}
          </button>
          <button hlmBtn variant="destructive" (click)="deleteTorrent(torrent)">Delete</button>
        </div>
      </div>
    </div>
  `,
})
export class TorrentTableComponent {
  protected hlmMuted = hlmMuted;
  protected readonly _selectedTorrent = signal<Torrent | null>(null);
  protected readonly _rawFilterInput = signal('');
  protected readonly _nameFilter = signal('');
  private readonly _debouncedFilter = toSignal(toObservable(this._rawFilterInput).pipe(debounceTime(300)));

  private readonly _displayedIndices = signal({ start: 0, end: 0 });
  protected readonly _availablePageSizes = [5, 10, 20, 10000];
  protected readonly _pageSize = signal(this._availablePageSizes[0]);

  private readonly _selectionModel = new SelectionModel<Torrent>(true);
  protected readonly _isTorrentSelected = (torrent: Torrent) => this._selectionModel.isSelected(torrent);
  protected readonly _selected = toSignal(this._selectionModel.changed.pipe(map((change) => change.source.selected)), {
    initialValue: [],
  });

  protected readonly _brnColumnManager = useBrnColumnManager({
    name: { visible: true, label: 'Name' },
    size: { visible: true, label: 'Size' },
    progress: { visible: true, label: 'Progress' },
    status: { visible: true, label: 'Status' },
    downloadSpeed: { visible: true, label: 'Download Speed' },
    uploadSpeed: { visible: true, label: 'Upload Speed' },
    peers: { visible: true, label: 'Peers' },
    dateAdded: { visible: true, label: 'Date Added' },
  });
  protected readonly _allDisplayedColumns = computed(() => [
    'select',
    ...this._brnColumnManager.displayedColumns(),
    'actions',
  ]);

  private readonly _torrents = signal<Torrent[]>([]);
  private readonly _filteredTorrents = computed(() => {
    const nameFilter = this._nameFilter()?.trim()?.toLowerCase();
    if (nameFilter && nameFilter.length > 0) {
      return this._torrents().filter((t) => t.name.toLowerCase().includes(nameFilter));
    }
    return this._torrents();
  });
  private readonly _nameSort = signal<'ASC' | 'DESC' | null>(null);
  protected readonly _filteredSortedPaginatedTorrents = computed(() => {
    const sort = this._nameSort();
    const start = this._displayedIndices().start;
    const end = this._displayedIndices().end + 1;
    const torrents = this._filteredTorrents();
    if (!sort) {
      return torrents.slice(start, end);
    }
    return [...torrents]
      .sort((t1, t2) => (sort === 'ASC' ? 1 : -1) * t1.name.localeCompare(t2.name))
      .slice(start, end);
  });
  protected readonly _allFilteredPaginatedTorrentsSelected = computed(() =>
    this._filteredSortedPaginatedTorrents().every((torrent) => this._selected().includes(torrent)),
  );
  protected readonly _checkboxState = computed(() => {
    const noneSelected = this._selected().length === 0;
    const allSelectedOrIndeterminate = this._allFilteredPaginatedTorrentsSelected() ? true : 'indeterminate';
    return noneSelected ? false : allSelectedOrIndeterminate;
  });

  protected readonly _trackBy: TrackByFunction<Torrent> = (_: number, t: Torrent) => t.id;
  protected readonly _totalElements = computed(() => this._filteredTorrents().length);
  protected readonly _onStateChange = ({ startIndex, endIndex }: PaginatorState) =>
    this._displayedIndices.set({ start: startIndex, end: endIndex });

  constructor(private torrentService: TorrentManagerService) {
    effect(() => this._nameFilter.set(this._debouncedFilter() ?? ''), { allowSignalWrites: true });

    this.torrentService.getTorrents().subscribe(torrents => {
      this._torrents.set(torrents);
    });
  }

  protected toggleTorrent(torrent: Torrent) {
    this._selectionModel.toggle(torrent);
  }

  protected handleHeaderCheckboxChange() {
    const previousCbState = this._checkboxState();
    if (previousCbState === 'indeterminate' || !previousCbState) {
      this._selectionModel.select(...this._filteredSortedPaginatedTorrents());
    } else {
      this._selectionModel.deselect(...this._filteredSortedPaginatedTorrents());
    }
  }

  protected handleNameSortChange() {
    const sort = this._nameSort();
    if (sort === 'ASC') {
      this._nameSort.set('DESC');
    } else if (sort === 'DESC') {
      this._nameSort.set(null);
    } else {
      this._nameSort.set('ASC');
    }
  }

  protected togglePause(torrent: Torrent) {
    if (torrent.status === 'paused') {
      this.torrentService.resumeTorrent(torrent.id).subscribe();
    } else {
      this.torrentService.pauseTorrent(torrent.id).subscribe();
    }
  }

  protected deleteTorrent(torrent: Torrent) {
    if (confirm(`Are you sure you want to delete "${torrent.name}"?`)) {
      this.torrentService.removeTorrent(torrent.id).subscribe();
    }
  }

  protected formatSize(bytes: number): string {
    const sizes = ['Bytes', 'KB', 'MB', 'GB', 'TB'];
    if (bytes === 0) return '0 Byte';
    const i = parseInt(Math.floor(Math.log(bytes) / Math.log(1024)).toString());
    return Math.round(bytes / Math.pow(1024, i)) + ' ' + sizes[i];
  }

  protected formatSpeed(bytesPerSecond: number): string {
    return this.formatSize(bytesPerSecond) + '/s';
  }

  protected selectTorrent(torrent: Torrent) {
    this._selectedTorrent.set(torrent);
  }

  protected closePanel() {
    this._selectedTorrent.set(null);
  }
}